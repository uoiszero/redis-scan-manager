import { Redis, Cluster } from 'ioredis';

interface RedisScanManagerOptions {
    redis: Redis | Cluster | any;
    indexPrefix?: string;
    hashChars?: number;
    scanBatchSize?: number;
    mgetBatchSize?: number;
}
interface DebugStats {
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
        maxBucket: {
            suffix: string;
            count: number;
        };
        minBucket: {
            suffix: string;
            count: number;
        };
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
declare class RedisScanManager {
    private redis;
    private isCluster;
    private redisConfig;
    private indexPrefix;
    private hashChars;
    private SCAN_BATCH_SIZE;
    private MGET_BATCH_SIZE;
    private buckets;
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
    constructor(options: RedisScanManagerOptions);
    /**
     * 内部方法：初始化 Lua 脚本
     * @private
     */
    private _initLuaScripts;
    /**
     * 内部方法：确保 Redis 连接已建立 (Lazy Connect)
     * @private
     */
    private _ensureConnection;
    /**
     * 内部方法：推导 Key 的字典序范围
     * @private
     * @param {string} startKey - 起始 Key
     * @param {string} [endKey] - 结束 Key
     * @returns {{lexStart: string, lexEnd: string}} Redis ZSET 字典序范围
     */
    private _inferRange;
    /**
     * 内部方法：根据后缀获取桶的完整 Key
     * @private
     * @param {string} suffix - 桶后缀
     * @returns {string} 完整桶 Key
     */
    private _getBucketName;
    /**
     * 计算 Key 所属的桶名
     *
     * @private
     * @param {string} key - 原始 Key
     * @returns {string} 桶的完整 Key (prefix + hashSuffix)
     */
    private _getBucketKey;
    /**
     * 内部方法：执行原子操作 (Lua 脚本或降级 Pipeline)
     * @private
     * @param {string} scriptName - Lua 脚本方法名
     * @param {Array<string>} keys - Redis Keys
     * @param {Array<string>} args - Lua 脚本参数
     * @param {Function} fallbackFn - 降级 Pipeline 构建函数
     */
    private _execAtomic;
    /**
     * 内部方法：构建批处理 Pipeline
     * @private
     * @param {Array<string>} bucketBatch - 桶后缀批次
     * @param {Function} callback - (pipeline, bucketKey) => void
     * @returns {Object} pipeline 对象
     */
    private _buildBatchPipeline;
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
    add(key: string, value: string): Promise<void>;
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
    del(keys: string | string[]): Promise<void>;
    /**
     * 范围扫描 (Scatter-Gather Scan) - 内存优化版
     *
     * 并发扫描所有分桶，查找符合字典序范围 [startKey, endKey] 的 Keys。
     * 采用分批合并策略，有效控制内存占用，避免 OOM。
     *
     * @param {string} startKey - 起始 Key (包含)，例如 "user:1000"
     * @param {string} [endKey] - 结束 Key (包含)。如果未提供，必须保证 startKey 能推导出前缀范围。
     * @param {number} [limit=100] - 返回结果的最大数量 (1-1000)。注意：这是全局 Limit。
     * @returns {Promise<Array<string>>} Key 和 Value 交替排列的扁平数组 [key1, val1, key2, val2...]
     * @throws {Error} 如果 limit 不合法或无法推导 endKey 范围时抛出异常
     */
    scan(startKey: string, endKey?: string, limit?: number): Promise<string[]>;
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
    count(startKey: string, endKey?: string): Promise<number>;
    /**
     * 获取索引调试统计信息
     *
     * 用于分析分桶的健康状况，例如总数据量、每个桶的负载、是否存在数据倾斜等。
     *
     * @param {boolean} [details=false] - 是否返回每个桶的详细数据量 (可能会比较大)
     * @returns {Promise<Object>} 统计信息对象
     */
    getDebugStats(details?: boolean): Promise<DebugStats>;
}

export { RedisScanManager };
