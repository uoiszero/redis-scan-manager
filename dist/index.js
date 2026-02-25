import { Redis } from 'ioredis';
import calculateSlot from 'cluster-key-slot';
import debug from 'debug';
import crypto from 'crypto';

// src/RedisScanManager.ts

// src/lua/scripts.ts
var ADD_INDEX_SCRIPT = `
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('ZADD', KEYS[2], 0, KEYS[1])
`;
var M_DEL_INDEX_SCRIPT = `
  for i=1, #KEYS, 2 do
    redis.call('DEL', KEYS[i])
    redis.call('ZREM', KEYS[i+1], KEYS[i])
  end
`;

// src/utils/key-range.ts
function inferRange(startKey, endKey) {
  const lexStart = `[${startKey}`;
  let lexEnd;
  if (!endKey) {
    const match = startKey.match(/^([a-zA-Z0-9]+)(_|:|-|\/|#)/);
    if (match) {
      const prefix = match[0];
      const lastChar = prefix.slice(-1);
      const nextChar = String.fromCharCode(lastChar.charCodeAt(0) + 1);
      const nextPrefix = prefix.slice(0, -1) + nextChar;
      lexEnd = `(${nextPrefix}`;
    } else {
      throw new Error(
        "Cannot infer endKey from startKey. Please provide an explicit endKey to avoid full scan."
      );
    }
  } else {
    lexEnd = `[${endKey}`;
  }
  return { lexStart, lexEnd };
}
function getBucketName(indexPrefix, suffix) {
  return `${indexPrefix}${suffix}`;
}
function getBucketKey(key, indexPrefix, hashChars) {
  const hash = crypto.createHash("md5").update(key).digest("hex");
  const bucketSuffix = hash.substring(0, hashChars);
  return getBucketName(indexPrefix, bucketSuffix);
}

// src/RedisScanManager.ts
var log = debug("redis-scan-manager");
var RedisScanManager = class {
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
  constructor(options) {
    this.isCluster = false;
    if (options.redis && typeof options.redis.pipeline === "function") {
      this.redis = options.redis;
      this.isCluster = !!this.redis.isCluster;
      this._initLuaScripts();
    } else {
      this.redisConfig = options.redis;
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
    const bucketCount = Math.pow(16, this.hashChars);
    this.SCAN_BATCH_SIZE = options.scanBatchSize || bucketCount;
    this.MGET_BATCH_SIZE = options.mgetBatchSize || 200;
    this.buckets = [];
    for (let i = 0; i < bucketCount; i++) {
      this.buckets.push(i.toString(16).padStart(this.hashChars, "0"));
    }
    this.nodeCache = /* @__PURE__ */ new Map();
  }
  /**
   * 内部方法：初始化 Lua 脚本
   * @private
   */
  _initLuaScripts() {
    if (typeof this.redis.defineCommand === "function") {
      if (typeof this.redis.addIndex !== "function") {
        this.redis.defineCommand("addIndex", {
          numberOfKeys: 2,
          lua: ADD_INDEX_SCRIPT
        });
      }
      if (typeof this.redis.mDelIndex !== "function") {
        this.redis.defineCommand("mDelIndex", {
          lua: M_DEL_INDEX_SCRIPT
        });
      }
    }
  }
  /**
   * 内部方法：确保 Redis 连接已建立 (Lazy Connect)
   * @private
   */
  async _ensureConnection() {
    if (!this.redis) {
      if (this.isCluster) {
        this.redis = new Redis.Cluster(this.redisConfig);
      } else {
        this.redis = new Redis(this.redisConfig);
      }
      this._initLuaScripts();
    }
  }
  /**
   * 内部方法：执行原子操作 (Lua 脚本或降级 Pipeline)
   * @private
   * @param {string} scriptName - Lua 脚本方法名
   * @param {Array<string>} keys - Redis Keys
   * @param {Array<string>} args - Lua 脚本参数
   * @param {Function} fallbackFn - 降级 Pipeline 构建函数
   */
  async _execAtomic(scriptName, keys, args, fallbackFn) {
    await this._ensureConnection();
    if (typeof this.redis[scriptName] === "function") {
      await this.redis[scriptName](...keys, ...args);
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
   * 内部方法：获取 Key 对应的 Master 节点 (Cluster 模式)
   * @private
   */
  _getNode(key) {
    if (!this.isCluster) {
      return this.redis;
    }
    if (this.nodeCache.has(key)) {
      return this.nodeCache.get(key);
    }
    const slot = calculateSlot(key);
    const nodeKey = this.redis.slots[slot][0];
    const node = this.redis.connectionPool.getInstanceByKey(nodeKey);
    this.nodeCache.set(key, node);
    return node;
  }
  /**
   * 内部方法：清理节点缓存
   * @private
   */
  _clearNodeCache() {
    this.nodeCache.clear();
  }
  /**
   * 内部方法：批量执行命令 (自动适配 Cluster 和 Pipeline)
   * @private
   * @param {Array<string>} bucketBatch - 桶后缀批次
   * @param {Function} commandFn - (client, bucketKey) => Promise | void
   * @returns {Promise<Array<[Error|null, any]>>}
   */
  async _runBatchCommand(bucketBatch, commandFn) {
    if (this.isCluster) {
      const nodesMap = /* @__PURE__ */ new Map();
      bucketBatch.forEach((suffix, index) => {
        const bucketKey = getBucketName(this.indexPrefix, suffix);
        const node = this._getNode(bucketKey);
        if (!nodesMap.has(node)) {
          nodesMap.set(node, { keys: [], indices: [] });
        }
        const group = nodesMap.get(node);
        group.keys.push(bucketKey);
        group.indices.push(index);
      });
      const results = new Array(bucketBatch.length);
      const promises = Array.from(nodesMap.entries()).map(
        async ([node, { keys, indices }]) => {
          const pipeline = node.pipeline();
          keys.forEach((bucketKey) => {
            commandFn(pipeline, bucketKey);
          });
          try {
            const pipelineResults = await pipeline.exec();
            if (pipelineResults) {
              pipelineResults.forEach((res, i) => {
                results[indices[i]] = res;
              });
            } else {
              indices.forEach(
                (idx) => results[idx] = [new Error("Pipeline returned null"), null]
              );
            }
          } catch (err) {
            indices.forEach((idx) => {
              results[idx] = [err, null];
            });
          }
        }
      );
      log(
        `[Batch Command] Buckets: ${bucketBatch.length}, Nodes (Pipelines): ${nodesMap.size}, Individual Requests: 0`
      );
      await Promise.all(promises);
      return results;
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
  async add(key, value) {
    const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);
    if (this.isCluster) {
      const keyNode = this._getNode(key);
      const bucketNode = this._getNode(bucketKey);
      const keyNodeHost = keyNode?.options?.host || "unknown";
      const bucketNodeHost = bucketNode?.options?.host || "unknown";
      const sameNode = keyNode && bucketNode && keyNode === bucketNode;
      log(
        `[Add] Key: ${key}, Bucket: ${bucketKey}, KeyNode: ${keyNodeHost}, BucketNode: ${bucketNodeHost}, SameNode: ${sameNode}`
      );
      if (sameNode) {
        const pipeline = keyNode.pipeline();
        pipeline.set(key, value);
        pipeline.zadd(bucketKey, 0, key);
        await pipeline.exec();
      } else {
        const promises = [];
        if (keyNode) {
          promises.push(keyNode.set(key, value));
        } else {
          promises.push(this.redis.set(key, value));
        }
        if (bucketNode) {
          promises.push(bucketNode.zadd(bucketKey, 0, key));
        } else {
          promises.push(this.redis.zadd(bucketKey, 0, key));
        }
        try {
          await Promise.all(promises);
        } catch (e) {
          console.error("Add Error", e);
        }
      }
    } else {
      await this._execAtomic(
        "addIndex",
        [key, bucketKey],
        [value],
        (pipeline) => {
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
  async del(keys) {
    this._clearNodeCache();
    let keysArray = [];
    if (typeof keys === "string") {
      keysArray = [keys];
    } else if (Array.isArray(keys)) {
      keysArray = keys;
    }
    if (keysArray.length === 0) {
      return;
    }
    console.log(`[Del] Total keys to delete: ${keysArray.length}`);
    const BATCH_SIZE = 1e3;
    for (let i = 0; i < keysArray.length; i += BATCH_SIZE) {
      const batchStart = Date.now();
      const batchKeys = keysArray.slice(i, i + BATCH_SIZE);
      log(`[Batch Delete] keys in Array: ${keysArray.length}`);
      if (this.isCluster) {
        const pipelines = /* @__PURE__ */ new Map();
        const individualPromises = [];
        const getPipeline = (node) => {
          if (!pipelines.has(node)) {
            pipelines.set(node, node.pipeline());
          }
          return pipelines.get(node);
        };
        for (const key of batchKeys) {
          const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);
          const keyNode = this._getNode(key);
          const bucketNode = this._getNode(bucketKey);
          const keyNodeHost = keyNode?.options?.host || "unknown";
          const bucketNodeHost = bucketNode?.options?.host || "unknown";
          const sameNode = keyNode && bucketNode && keyNode === bucketNode;
          log(
            `[Del] Key: ${key}, Bucket: ${bucketKey}, KeyNode: ${keyNodeHost}, BucketNode: ${bucketNodeHost}, SameNode: ${sameNode}`
          );
          try {
            if (keyNode) {
              getPipeline(keyNode).del(key);
            } else {
              individualPromises.push(this.redis.del(key));
            }
          } catch (e) {
            individualPromises.push(this.redis.del(key));
          }
          try {
            if (bucketNode) {
              getPipeline(bucketNode).zrem(bucketKey, key);
            } else {
              individualPromises.push(this.redis.zrem(bucketKey, key));
            }
          } catch (e) {
            individualPromises.push(this.redis.zrem(bucketKey, key));
          }
        }
        log(
          `[Batch Delete] Keys: ${batchKeys.length}, Nodes (Pipelines): ${pipelines.size}, Individual Requests: ${individualPromises.length}`
        );
        const execStart = Date.now();
        await Promise.all([
          ...Array.from(pipelines.values()).map((p) => p.exec()),
          ...individualPromises
        ]);
        const batchDuration = Date.now() - batchStart;
        const execDuration = Date.now() - execStart;
        console.log(`[Del] Batch delete completed in ${batchDuration}ms (Pipeline exec: ${execDuration}ms)`);
      } else {
        const keysAndBuckets = [];
        for (const key of batchKeys) {
          const bucketKey = getBucketKey(key, this.indexPrefix, this.hashChars);
          keysAndBuckets.push(key, bucketKey);
        }
        await this._execAtomic(
          "mDelIndex",
          [String(keysAndBuckets.length), ...keysAndBuckets],
          [],
          (pipeline) => {
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
  async scan(startKey, endKey, limit = 100, keysOnly = false) {
    await this._ensureConnection();
    if (!Number.isInteger(limit) || limit < 1 || limit > 1e3) {
      throw new Error("Limit must be an integer between 1 and 1000");
    }
    const { lexStart, lexEnd } = inferRange(startKey, endKey);
    const batchResults = await this._runBatchCommand(
      this.buckets,
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
    let allKeys = [];
    if (batchResults) {
      for (const [err, keys] of batchResults) {
        if (err) {
          console.error("Scan error:", err);
          continue;
        }
        if (keys && keys.length > 0) {
          allKeys.push(...keys);
        }
      }
    }
    if (allKeys.length > 0) {
      allKeys.sort();
      if (allKeys.length > limit) {
        allKeys = allKeys.slice(0, limit);
      }
    }
    if (allKeys.length === 0) {
      return [];
    }
    if (keysOnly) {
      const result2 = [];
      for (const key of allKeys) {
        result2.push(key, "");
      }
      return result2;
    }
    const valuePromises = [];
    for (let i = 0; i < allKeys.length; i += this.MGET_BATCH_SIZE) {
      const batchKeys = allKeys.slice(i, i + this.MGET_BATCH_SIZE);
      if (this.isCluster) {
        valuePromises.push(Promise.all(batchKeys.map((k) => this.redis.get(k))));
      } else {
        valuePromises.push(this.redis.mget(batchKeys));
      }
    }
    const mgetResults = await Promise.all(valuePromises);
    const values = mgetResults.flat();
    const result = [];
    for (let i = 0; i < allKeys.length; i++) {
      result.push(allKeys[i], values[i]);
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
  async count(startKey, endKey) {
    await this._ensureConnection();
    const { lexStart, lexEnd } = inferRange(startKey, endKey);
    const batchResults = await this._runBatchCommand(
      this.buckets,
      (client, bucketKey) => {
        return client.zlexcount(bucketKey, lexStart, lexEnd);
      }
    );
    let totalCount = 0;
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
  async getDebugStats(details = false) {
    await this._ensureConnection();
    const results = await this._runBatchCommand(
      this.buckets,
      (client, bucketKey) => {
        return client.zcard(bucketKey);
      }
    );
    let totalItems = 0;
    let minItems = Number.MAX_SAFE_INTEGER;
    let maxItems = 0;
    let emptyBuckets = 0;
    let minBucketSuffix = "";
    let maxBucketSuffix = "";
    const bucketsData = {};
    if (results) {
      for (let i = 0; i < results.length; i++) {
        const [err, count] = results[i];
        const suffix = this.buckets[i];
        if (err) {
          console.error(`Error getting ZCARD for bucket ${suffix}:`, err);
          continue;
        }
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
    if (totalItems === 0) {
      minItems = 0;
    }
    const totalBuckets = this.buckets.length;
    const avgItems = totalBuckets > 0 ? totalItems / totalBuckets : 0;
    const stats = {
      meta: {
        hashChars: this.hashChars,
        totalBuckets,
        indexPrefix: this.indexPrefix
      },
      stats: {
        totalItems,
        avgItems: parseFloat(avgItems.toFixed(2)),
        minItems,
        maxItems,
        emptyBuckets
      },
      outliers: {
        maxBucket: { suffix: maxBucketSuffix, count: maxItems },
        minBucket: { suffix: minBucketSuffix, count: minItems }
      }
    };
    if (details) {
      stats.buckets = bucketsData;
    }
    return stats;
  }
};

export { RedisScanManager };
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map