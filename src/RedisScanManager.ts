import { Redis, type Cluster, type ChainableCommander } from "ioredis";
import { ADD_INDEX_SCRIPT, M_DEL_INDEX_SCRIPT } from "./lua/scripts.js";
import { inferRange } from "./utils/key-range.js";
import { getBucketKey, getBucketName } from "./utils/sharding.js";

export interface RedisScanManagerOptions {
  redis: Redis | Cluster | any;
  indexPrefix?: string;
  hashChars?: number;
  scanBatchSize?: number;
  mgetBatchSize?: number;
}

export interface DebugStats {
  meta: {
    hashChars: number;
    totalBuckets: number;
    indexPrefix: string;
  };
  stats: {
    totalItems: number;
    avgItems: number;
    minItems: number;
    maxItems: number;
    emptyBuckets: number;
  };
  outliers: {
    maxBucket: { suffix: string; count: number };
    minBucket: { suffix: string; count: number };
  };
  buckets?: Record<string, number>;
}

/**
 * Redis 二级索引管理器 (优化版)
 *
 * 该类通过 Hash 分桶策略实现了高效的 Redis 二级索引管理，解决了海量数据下的索引热点问题。
 *
 * 核心特性：
 * 1. **分桶策略**：对 Key 进行 Hash 后取前 N 位 (Hex) 分桶，将索引均匀打散到多个 ZSET 中。
 * 2. **Scatter-Gather 查询**：范围查询时并发扫描所有相关桶，并在内存中进行归并排序。
 * 3. **内存保护**：Scan 采用分批合并 (Incremental Merge) 策略，防止 Limit 放大效应导致的内存溢出。
 * 4. **原子操作**：使用 Lua 脚本保证索引与源数据的一致性。
 *
 * **Redis Cluster 注意事项**：
 * 本模块的原子性依赖 Lua 脚本同时操作 Data Key 和 Index Bucket Key。
 * 在 Redis Cluster 模式下，除非这两个 Key 在同一个 Slot (使用 Hash Tag {...})，否则 Lua 脚本会报错 `CROSSSLOT`。
 * 由于本模块采用 Hash 分桶策略，Data Key 和 Bucket Key 天然很难在同一个 Slot。
 * 因此，**本模块默认仅适用于单机 Redis 或支持多 Key 事务的代理环境**。
 * 如果需要在 Cluster 下使用，建议放弃原子性，修改 `add/del` 为分步操作。
 *
 * @example
 * const manager = new RedisScanManager({ redis: redisClient, hashChars: 2 });
 * await manager.add("user:1001", JSON.stringify({ name: "Alice" }));
 * const users = await manager.scan("user:1000", "user:1005", 10);
 */
export class RedisScanManager {
  private redis!: Redis | Cluster;
  private isCluster: boolean;
  private redisConfig: any;
  private indexPrefix: string;
  private hashChars: number;
  private SCAN_BATCH_SIZE: number;
  private MGET_BATCH_SIZE: number;
  private buckets: string[];

  /**
   * @param {Object} options
   * @param {Redis|Object} [options.redis] - ioredis 实例，或 ioredis 构造函数参数 (配置对象)
   * @param {string} [options.indexPrefix="idx:"] - 索引 Key 的前缀
   * @param {number} [options.hashChars=2] - Hash 分桶取前几位 (Hex)
   * @param {number} [options.scanBatchSize=50] - Scan 批处理大小
   * @param {number} [options.mgetBatchSize=200] - MGET 批处理大小
   *
   * @description
   * **适用场景说明**：
   * 本模块专为 **千万级及以上** 海量数据场景设计。
   * 由于采用了 Hash 分桶和 Scatter-Gather (分散-聚合) 查询策略，会产生多次网络往返和并发开销。
   * 如果数据量较小（例如少于 10 万条），直接使用单个 Redis ZSET 的性能通常优于本方案，不建议使用此管理器。
   */
  constructor(options: RedisScanManagerOptions) {
    // 检查 options.redis 是实例还是配置
    // ioredis 实例通常有 pipeline 方法
    this.isCluster = false;

    if (options.redis && typeof options.redis.pipeline === "function") {
      this.redis = options.redis;
      // ioredis 的 Cluster 实例通常有 isCluster 属性
      this.isCluster = !!(this.redis as any).isCluster;
      this._initLuaScripts();
    } else {
      // 视为配置对象，保存以备懒加载
      this.redisConfig = options.redis;
      // 如果配置是数组，说明是 Cluster 节点列表
      if (Array.isArray(options.redis)) {
        this.isCluster = true;
      }
    }

    this.indexPrefix = options.indexPrefix || "idx:";

    const hashChars = options.hashChars || 2;
    if (hashChars !== 1 && hashChars !== 2) {
      throw new Error("options.hashChars must be 1 or 2");
    }
    this.hashChars = hashChars;

    // 配置项: 批处理大小
    this.SCAN_BATCH_SIZE = options.scanBatchSize || 50;
    this.MGET_BATCH_SIZE = options.mgetBatchSize || 200;

    this.buckets = [];
    const maxVal = Math.pow(16, this.hashChars);
    for (let i = 0; i < maxVal; i++) {
      this.buckets.push(i.toString(16).padStart(this.hashChars, "0"));
    }
  }

  /**
   * 内部方法：初始化 Lua 脚本
   * @private
   */
  private _initLuaScripts() {
    if (typeof (this.redis as any).defineCommand === "function") {
      // 避免重复定义 (检查 this.redis.addIndex 是否为函数)
      if (typeof (this.redis as any).addIndex !== "function") {
        (this.redis as any).defineCommand("addIndex", {
          numberOfKeys: 2,
          lua: ADD_INDEX_SCRIPT,
        });
      }

      if (typeof (this.redis as any).mDelIndex !== "function") {
        (this.redis as any).defineCommand("mDelIndex", {
          lua: M_DEL_INDEX_SCRIPT,
        });
      }
    }
  }

  /**
   * 内部方法：确保 Redis 连接已建立 (Lazy Connect)
   * @private
   */
  private async _ensureConnection() {
    if (!this.redis) {
      if (this.isCluster) {
        this.redis = new Redis.Cluster(this.redisConfig);
      } else {
        this.redis = new Redis(this.redisConfig);
      }
      this._initLuaScripts();
    }
    // 如果是 Lazy Connect 模式，ioredis 会在第一个命令时自动连接，无需显式调用 connect
  }

  /**
   * 内部方法：执行原子操作 (Lua 脚本或降级 Pipeline)
   * @private
   * @param {string} scriptName - Lua 脚本方法名
   * @param {Array<string>} keys - Redis Keys
   * @param {Array<string>} args - Lua 脚本参数
   * @param {Function} fallbackFn - 降级 Pipeline 构建函数
   */
  private async _execAtomic(
    scriptName: string,
    keys: string[],
    args: string[],
    fallbackFn: (pipeline: ChainableCommander) => void
  ) {
    await this._ensureConnection();
    if (typeof (this.redis as any)[scriptName] === "function") {
      await (this.redis as any)[scriptName](...keys, ...args);
    } else {
      console.warn(
        "[RedisIndexManager] Lua scripts not supported. Falling back to non-atomic pipeline. Data consistency is NOT guaranteed."
      );
      const pipeline = this.redis.pipeline();
      fallbackFn(pipeline);
      await pipeline.exec();
    }
  }

  /**
   * 内部方法：批量执行命令 (自动适配 Cluster 和 Pipeline)
   * @private
   * @param {Array<string>} bucketBatch - 桶后缀批次
   * @param {Function} commandFn - (client, bucketKey) => Promise | void
   * @returns {Promise<Array<[Error|null, any]>>}
   */
  private async _runBatchCommand(
    bucketBatch: string[],
    commandFn: (client: any, bucketKey: string) => any
  ): Promise<[Error | null, any][]> {
    if (this.isCluster) {
      const promises = bucketBatch.map(async (suffix) => {
        const bucketKey = getBucketName(this.indexPrefix, suffix);
        try {
          const res = await commandFn(this.redis, bucketKey);
          return [null, res] as [null, any];
        } catch (err) {
          return [err as Error, undefined] as [Error, any];
        }
      });
      return Promise.all(promises);
    } else {
      const pipeline = this.redis.pipeline();
      for (const suffix of bucketBatch) {
        const bucketKey = getBucketName(this.indexPrefix, suffix);
        commandFn(pipeline, bucketKey);
      }
      const results = await pipeline.exec();
      return results || [];
    }
  }

  /**
   * 添加或更新数据及其索引 (原子操作)
   *
   * 使用 Lua 脚本同时更新 KV 数据和 ZSET 索引，确保两者的一致性。
   * 如果 Key 已存在，将覆盖原有 Value 并更新索引（Score 固定为 0）。
   *
   * @param {string} key - 数据的唯一标识 (如 "user:1001")
   * @param {string} value - 数据内容 (字符串或序列化后的 JSON)
   * @returns {Promise<void>}
   */
  async add(key: string, value: string) {
    const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);

    if (this.isCluster) {
      // Cluster 模式：分步执行，无法保证原子性，但能避免 CROSSSLOT 错误
      // 并行执行以提高效率
      await Promise.all([
        this.redis.set(key, value),
        this.redis.zadd(bucketKey, 0, key),
      ]);
    } else {
      await this._execAtomic(
        "addIndex",
        [key, bucketKey],
        [value],
        pipeline => {
          pipeline.set(key, value);
          pipeline.zadd(bucketKey, 0, key);
        }
      );
    }
  }

  /**
   * 删除数据及其索引 (支持单条或批量原子删除)
   *
   * 自动识别参数类型：
   * - 传入字符串：作为单个 Key 删除
   * - 传入字符串数组：作为多个 Key 批量删除
   *
   * 使用 Lua 脚本一次性删除多个 KV 数据和 ZSET 中的索引条目。
   *
   * @param {string|Array<string>} keys - 待删除的 key 或 keys 数组
   * @returns {Promise<void>}
   */
  async del(keys: string | string[]) {
    // 兼容单字符串参数
    let keysArray: string[] = [];
    if (typeof keys === "string") {
      keysArray = [keys];
    } else if (Array.isArray(keys)) {
      keysArray = keys;
    }

    if (keysArray.length === 0) {
      return;
    }

    // Fix: Process in batches to avoid stack overflow
    const BATCH_SIZE = 1000;

    for (let i = 0; i < keysArray.length; i += BATCH_SIZE) {
      const batchKeys = keysArray.slice(i, i + BATCH_SIZE);

      if (this.isCluster) {
        // Cluster 模式：并发执行，避免 CROSSSLOT 错误
        const promises: Promise<any>[] = [];
        for (const key of batchKeys) {
          const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);
          promises.push(this.redis.del(key));
          promises.push(this.redis.zrem(bucketKey, key));
        }
        await Promise.all(promises);
      } else {
        const keysAndBuckets: string[] = [];
        for (const key of batchKeys) {
          const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);
          keysAndBuckets.push(key, bucketKey);
        }

        // ioredis 自定义命令如果不指定 numberOfKeys，第一个参数必须是 key 的数量
        // 所有的参数都是 Key (key, bucketKey, key, bucketKey...)
        await this._execAtomic(
          "mDelIndex",
          [String(keysAndBuckets.length), ...keysAndBuckets],
          [],
          pipeline => {
            for (let j = 0; j < keysAndBuckets.length; j += 2) {
              const key = keysAndBuckets[j];
              const bucketKey = keysAndBuckets[j + 1];
              pipeline.del(key);
              pipeline.zrem(bucketKey, key);
            }
          }
        );
      }
    }
  }

  /**
   * 范围扫描 (Scatter-Gather Scan) - 内存优化版
   *
   * 并发扫描所有分桶，查找符合字典序范围 [startKey, endKey] 的 Keys。
   * 采用分批合并策略，有效控制内存占用，避免 OOM。
   *
   * @param {string} startKey - 起始 Key (包含)，例如 "user:1000"
   * @param {string} [endKey] - 结束 Key (包含)。如果未提供，必须保证 startKey 能推导出前缀范围。
   * @param {number} [limit=100] - 返回结果的最大数量 (1-1000)。注意：这是全局 Limit。
   * @param {boolean} [keysOnly=false] - 是否只返回 Key (Value 为 null)，用于快速扫描或删除
   * @returns {Promise<Array<string>>} Key 和 Value 交替排列的扁平数组 [key1, val1, key2, val2...]
   * @throws {Error} 如果 limit 不合法或无法推导 endKey 范围时抛出异常
   */
  async scan(startKey: string, endKey?: string, limit = 100, keysOnly = false) {
    await this._ensureConnection();
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("Limit must be an integer between 1 and 1000");
    }

    const { lexStart, lexEnd } = inferRange(startKey, endKey);

    // console.log(`[Scan Optimized] Range: [${startKey}, ${endKey || "AUTO"}], Limit: ${limit}`);

    let allKeys: string[] = [];

    for (let i = 0; i < this.buckets.length; i += this.SCAN_BATCH_SIZE) {
      const bucketBatch = this.buckets.slice(i, i + this.SCAN_BATCH_SIZE);
      const batchResults = await this._runBatchCommand(
        bucketBatch,
        (client, bucketKey) => {
          return client.zrangebylex(
            bucketKey,
            lexStart,
            lexEnd,
            "LIMIT",
            0,
            limit
          );
        }
      );

      // 优化：分批合并 (Incremental Merge)
      let batchKeys: string[] = [];
      if (batchResults) {
        for (const [err, keys] of batchResults) {
          if (err) {
            console.error("Scan error:", err);
            continue;
          }
          if (keys && (keys as string[]).length > 0) {
            batchKeys.push(...(keys as string[]));
          }
        }
      }

      if (batchKeys.length > 0) {
        // 策略：将新的一批数据加入总集，然后立即排序并截断
        // 这样内存中永远只保留 Limit + BatchSize*Limit 的数据量
        allKeys = allKeys.concat(batchKeys);
        allKeys.sort();

        if (allKeys.length > limit) {
          allKeys = allKeys.slice(0, limit);
        }
      }
    }

    if (allKeys.length === 0) {
      return [];
    }

    if (keysOnly) {
      const result: string[] = [];
      for (const key of allKeys) {
        result.push(key, "");
      }
      return result;
    }

    const valuePromises = [];

    for (let i = 0; i < allKeys.length; i += this.MGET_BATCH_SIZE) {
      const batchKeys = allKeys.slice(i, i + this.MGET_BATCH_SIZE);
      if (this.isCluster) {
        // Cluster 模式下 MGET 无法跨 Slot，使用并发 GET 代替
        valuePromises.push(Promise.all(batchKeys.map((k) => this.redis.get(k))));
      } else {
        valuePromises.push(this.redis.mget(batchKeys));
      }
    }

    const batchResults = await Promise.all(valuePromises);
    const values = batchResults.flat();

    // 拍平为 [key1, val1, key2, val2, ...]
    const result: string[] = [];
    for (let i = 0; i < allKeys.length; i++) {
      result.push(allKeys[i], values[i] as string);
    }
    return result;
  }

  /**
   * 统计范围内的数据总数
   *
   * 利用 ZLEXCOUNT 高效统计所有分桶中符合范围的 Key 数量。
   * 这是一个纯服务端计算操作，无需拉取数据到内存，非常快速。
   *
   * @param {string} startKey - 起始 Key (包含)
   * @param {string} [endKey] - 结束 Key (包含)。自动推导逻辑同 scan。
   * @returns {Promise<number>} 数据总数
   */
  async count(startKey: string, endKey?: string) {
    await this._ensureConnection();

    const { lexStart, lexEnd } = inferRange(startKey, endKey);

    let totalCount = 0;

    // 并发执行所有批次
    const promises = [];

    for (let i = 0; i < this.buckets.length; i += this.SCAN_BATCH_SIZE) {
      const bucketBatch = this.buckets.slice(i, i + this.SCAN_BATCH_SIZE);
      promises.push(
        this._runBatchCommand(bucketBatch, (client, bucketKey) => {
          return client.zlexcount(bucketKey, lexStart, lexEnd);
        })
      );
    }

    const allBatchResults = await Promise.all(promises);

    for (const batchResults of allBatchResults) {
      if (batchResults) {
        for (const [err, count] of batchResults) {
          if (err) {
            console.error("Count error:", err);
            continue;
          }
          if (typeof count === "number") {
            totalCount += count;
          }
        }
      }
    }

    return totalCount;
  }

  /**
   * 获取索引调试统计信息
   *
   * 用于分析分桶的健康状况，例如总数据量、每个桶的负载、是否存在数据倾斜等。
   *
   * @param {boolean} [details=false] - 是否返回每个桶的详细数据量 (可能会比较大)
   * @returns {Promise<Object>} 统计信息对象
   */
  async getDebugStats(details = false): Promise<DebugStats> {
    await this._ensureConnection();

    // 1. 使用 Pipeline/Promise.all 批量获取所有桶的大小 (ZCARD)
    const results = await this._runBatchCommand(
      this.buckets,
      (client, bucketKey) => {
        return client.zcard(bucketKey);
      }
    );

    // 2. 统计分析
    let totalItems = 0;
    let minItems = Number.MAX_SAFE_INTEGER;
    let maxItems = 0;
    let emptyBuckets = 0;

    let minBucketSuffix = "";
    let maxBucketSuffix = "";

    const bucketsData: Record<string, number> = {};

    if (results) {
      for (let i = 0; i < results.length; i++) {
        const [err, count] = results[i];
        const suffix = this.buckets[i];

        if (err) {
          console.error(`Error getting ZCARD for bucket ${suffix}:`, err);
          continue;
        }

        // 确保 count 是数字
        const size = typeof count === "number" ? count : 0;

        totalItems += size;

        if (size === 0) {
          emptyBuckets++;
        }

        if (size < minItems) {
          minItems = size;
          minBucketSuffix = suffix;
        }

        if (size > maxItems) {
          maxItems = size;
          maxBucketSuffix = suffix;
        }

        if (details) {
          bucketsData[suffix] = size;
        }
      }
    }

    // 如果所有桶都为空，minItems 重置为 0
    if (totalItems === 0) {
      minItems = 0;
    }

    const totalBuckets = this.buckets.length;
    const avgItems = totalBuckets > 0 ? totalItems / totalBuckets : 0;

    // 3. 构建返回对象
    const stats: DebugStats = {
      meta: {
        hashChars: this.hashChars,
        totalBuckets: totalBuckets,
        indexPrefix: this.indexPrefix,
      },
      stats: {
        totalItems: totalItems,
        avgItems: parseFloat(avgItems.toFixed(2)),
        minItems: minItems,
        maxItems: maxItems,
        emptyBuckets: emptyBuckets,
      },
      outliers: {
        maxBucket: { suffix: maxBucketSuffix, count: maxItems },
        minBucket: { suffix: minBucketSuffix, count: minItems },
      },
    };

    if (details) {
      stats.buckets = bucketsData;
    }

    return stats;
  }
}
