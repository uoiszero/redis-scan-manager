# Redis Scan Manager

`redis-scan-manager` 是一个基于 Hash 分桶策略的高性能 Redis 二级索引管理器。它专为解决海量数据下的索引热点问题而设计，支持高效的范围查询（Range Scan）和原子性的数据操作。

**注意：本模块专为千万级及以上数据量设计。如果数据量较小（< 10万），建议直接使用原生 Redis ZSET，性能会更好。**

## 🌟 核心特性

- **Hash 分桶 (Sharding)**: 将索引数据均匀打散到多个 ZSET 中（默认 256 个桶），避免单 Key 热点问题。
- **Scatter-Gather 查询**: 范围查询时自动并发扫描所有相关桶，并在内存中进行归并排序。
- **内存保护**: 采用分批合并 (Incremental Merge) 策略，有效防止大范围 Scan 导致的内存溢出 (OOM)。
- **原子操作**: 在单机模式下，使用 Lua 脚本保证数据 (KV) 与索引 (ZSET) 的强一致性。
- **Cluster 支持**: 支持 Redis Cluster 环境（自动降级原子性以保证可用性）。

---

## 📦 安装

```bash
npm install redis-scan-manager
```

---

## 🚀 快速开始

### 1. 初始化

支持传入 ioredis 实例（兼容旧代码）或 Redis 配置对象（推荐，支持延迟连接）。

```javascript
import Redis from "ioredis";
import { RedisIndexManager } from "redis-scan-manager";

// 方式 A: 传入 ioredis 实例
const redis = new Redis();
const manager = new RedisIndexManager({
  redis: redis,
  indexPrefix: "idx:", 
  hashChars: 2
});

// 方式 B: 传入配置对象 (Lazy Connect)
// 连接会在第一次调用 add/scan 等方法时自动建立
const managerLazy = new RedisIndexManager({
  redis: {
    host: "127.0.0.1",
    port: 6379,
    // 其他 ioredis 配置...
  },
  indexPrefix: "idx:",
  hashChars: 2
});
```

### 2. 添加数据

```javascript
// 会同时在 Redis 写入 "user:1001" (String) 和 更新索引 (ZSET)
await manager.add("user:1001", JSON.stringify({ name: "Alice", age: 30 }));
```

### 3. 范围查询

```javascript
// 查询 user:1000 到 user:1005 之间的数据
// 返回格式: [key1, val1, key2, val2, ...]
const results = await manager.scan("user:1000", "user:1005", 10);

for (let i = 0; i < results.length; i += 2) {
  const key = results[i];
  const val = results[i + 1];
  console.log(key, val);
}
```

### 4. 删除数据

```javascript
// 原子删除数据和索引
await manager.del("user:1001");
```

---

## 📚 API 详解

### `constructor(options)`

| 参数 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `redis` | `Redis \| Object` | - | **必填**。ioredis 实例，或 ioredis 构造函数配置对象（支持 Lazy Connect）。 |
| `indexPrefix` | `string` | `"idx:"` | 索引 ZSET 的 Key 前缀。 |
| `hashChars` | `number` | `2` | Hash 分桶位数 (Hex)。<br>`1`: 16 桶 (适合千万级)<br>`2`: 256 桶 (适合亿级，推荐) |
| `scanBatchSize` | `number` | `50` | Scan 时并发 Pipeline 的批次大小。 |
| `mgetBatchSize` | `number` | `200` | 回查 Value 时 MGET 的批次大小。 |

### `async add(key, value)`

添加或更新数据及其索引。

- **Key**: 数据的唯一标识。
- **Value**: 数据内容（字符串）。
- **原子性**: 
  - **Standalone**: 使用 Lua 脚本保证原子性。
  - **Cluster**: 并行执行 `SET` 和 `ZADD`，**不保证原子性**（为了避免 Cross Slot 错误）。

### `async del(key | keys)`

删除数据和索引。

- **参数**: 支持单个 Key (String) 或 Key 数组 (Array<String>)。
- **说明**: 自动计算每个 Key 对应的桶并进行清理。

### `async scan(startKey, endKey, limit)`

执行分布式范围查询 (Scatter-Gather Scan)。

| 参数 | 类型 | 必填 | 说明 |
| :--- | :--- | :--- | :--- |
| `startKey` | `string` | 是 | 扫描起始 Key (包含)。 |
| `endKey` | `string` | 否 | 扫描结束 Key (包含)。<br>如果未提供，尝试根据 `startKey` 的分隔符 (`_`, `:`, `-`, `/`, `#`) 自动推导前缀范围。<br>推导失败将抛出错误。 |
| `limit` | `number` | 否 | **全局**返回数量限制 (默认 100)。<br>范围: 1 - 1000。 |

- **返回值**: `Promise<Array<string>>` (Key/Value 交替数组)。

### `async count(startKey, endKey)`

高效统计指定范围内的 Key 总数。

- **特性**: 纯服务端计算 (`ZLEXCOUNT`)，无需拉取数据，速度极快且不消耗 Node.js 内存。

### `async getDebugStats(details = false)`

获取索引的调试与统计信息，用于分析数据分布和倾斜情况。

- **details**: `boolean`，是否返回每个桶的具体大小（默认 false）。
- **返回值**: 
  ```json
  {
    "meta": { "totalBuckets": 256 },
    "stats": {
      "totalItems": 10000,
      "avgItems": 39.06,
      "minItems": 30,
      "maxItems": 50,
      "emptyBuckets": 0
    },
    "outliers": {
      "maxBucket": { "suffix": "a5", "count": 50 },
      "minBucket": { "suffix": "1b", "count": 30 }
    }
  }
  ```

---

## ⚠️ 限制与风险 (Limitations & Risks)

### 1. 读放大 (Read Amplification)
由于采用了 Hash 分桶策略，`scan` 方法必须扫描所有分桶（或按批次扫描）。
- **建议**: 避免在高频（QPS > 1000）场景下调用 `scan`。

### 2. Redis Cluster 原子性限制
- **说明**: 在 Cluster 模式下，由于 Data Key 和 Index Key 通常不在同一个 Slot，无法使用 Lua 脚本。
- **行为**: 代码会自动降级为并发执行 `SET` 和 `ZADD`。
- **风险**: 极端情况下（如写入时进程崩溃），可能导致“数据存在但索引丢失”或“索引存在但数据丢失”的不一致情况。

### 3. Limit 限制
- **限制**: `limit` 最大允许设置为 **1000**。
- **原因**: 防止 Node.js 进程在归并排序时发生 OOM。

---

## 💡 最佳实践

1. **Key 命名规范**: 使用分隔符（如 `user:1001`），以便 `scan` 自动推导范围。
2. **显式 endKey**: 在生产环境中，尽量显式传入 `endKey`，明确扫描范围。
3. **监控**: 使用 `getDebugStats()` 定期监控分桶的负载均衡情况，确保 Hash 算法没有导致严重的数据倾斜。
4. **Value 大小**: 尽量控制 Value 的大小。

---

## 🛠 开发与测试

项目包含完整的基准测试脚本，用于验证性能和一致性。

```bash
# 运行测试
node test/test_redis_index_manager.js
```
