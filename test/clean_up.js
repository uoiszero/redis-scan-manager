import Redis from "ioredis";
import { RedisScanManager } from "../dist/index.js";

const singletonConfig = {
  host: "localhost",
  port: 6379
};

const clusterConfig = [
  {
    host: "192.168.5.9",
    port: 51002
  },
  {
    host: "192.168.5.10",
    port: 51002
  },
  {
    host: "192.168.5.11",
    port: 51002
  }
];

const mode = process.env.TEST_MODE || "cluster";
console.log(`Running in ${mode} mode...`);
const config = mode === "cluster" ? clusterConfig : singletonConfig;

/**
 * 删除指定前缀的测试数据
 *
 * 采用流式清理策略 (Streaming Deletion)：
 * 1. 使用 scan() 方法分批次获取 Key。
 * 2. 获取一批后立即执行删除，释放内存。
 * 3. 清理完成后通过 count() 再次验证。
 *
 * @param {RedisScanManager} manager - RedisScanManager 实例
 * @param {string} dataPrefix - 待清理数据的 Key 前缀
 * @returns {Promise<number>} - 返回删除的键数量
 */
async function cleanupData(manager, dataPrefix) {
  console.log(`Starting cleanup for prefix: ${dataPrefix}`);

  let lastKey = undefined;
  let totalDeleted = 0;

  const MAX_LOOPS = 5000;
  const BATCH_SIZE = 500;

  const remainingCount = await manager.count(dataPrefix);
  console.log(`Keys before cleanup: ${remainingCount}`);

  console.time("Total cleanup time");

  let batchIndex = 0;
  while (totalDeleted < MAX_LOOPS * BATCH_SIZE) {
    const batchLabel = `Batch ${batchIndex++}`;

    console.time(batchLabel);

    let startKey = lastKey ? lastKey : dataPrefix;
    if (lastKey) {
      startKey = lastKey + "\x00";
    }

    const batchData = await manager.scan(startKey, undefined, BATCH_SIZE, true);

    if (batchData.length === 0) {
      console.timeEnd(batchLabel);
      break;
    }

    const batchKeysToDelete = [];
    for (let i = 0; i < batchData.length; i += 2) {
      batchKeysToDelete.push(batchData[i]);
    }

    const nextStartKey = batchData[batchData.length - 2];

    await manager.del(batchKeysToDelete);

    console.timeEnd(batchLabel);

    lastKey = nextStartKey;
    totalDeleted += batchKeysToDelete.length;

    const currentRemaining = await manager.count(dataPrefix);
    console.log(
      `Batch deleted ${batchKeysToDelete.length} keys, total deleted: ${totalDeleted}, remaining: ${currentRemaining}`
    );

    if (batchData.length < BATCH_SIZE * 2) {
      break;
    }
  }

  console.timeEnd("Total cleanup time");
  console.log(`Cleanup finished. Total deleted: ${totalDeleted} keys`);

  return totalDeleted;
}

/**
 * 主清理流程
 *
 * @returns {Promise<void>}
 */
async function runCleanup() {
  const dataPrefix = process.env.DATA_PREFIX;

  if (!dataPrefix) {
    console.error("请通过环境变量 DATA_PREFIX 指定要删除的数据前缀");
    console.error("用法: DATA_PREFIX=scan_test_user: node test/clean_up.js");
    process.exit(1);
  }

  console.log(`Cleaning up data with prefix: ${dataPrefix}`);
  console.log(`Mode: ${mode}`);

  let redis;
  const redisOptions = {
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    enableReadyCheck: false,
    retryStrategy: times => {
      if (times > 3) return null;
      return Math.min(times * 100, 2000);
    }
  };

  if (Array.isArray(config)) {
    console.log("Initializing Redis Cluster...");
    redis = new Redis.Cluster(config, {
      redisOptions: redisOptions,
      clusterRetryStrategy: times => {
        if (times > 3) return null;
        return Math.min(times * 100, 2000);
      }
    });
  } else {
    console.log("Initializing Redis Standalone...");
    redis = new Redis({ ...config, ...redisOptions });
  }

  redis.on("error", err => {
    console.error("Redis Client Error:", err);
  });

  console.log("Waiting for Redis connection...");
  await new Promise((resolve, reject) => {
    redis.once("ready", () => {
      console.log("Redis client is ready.");
      resolve();
    });

    setTimeout(() => {
      if (redis.status !== "ready") {
        reject(new Error("Redis connection timeout"));
      }
    }, 10000);
  });

  try {
    const pingResult = await redis.ping();
    console.log("Redis Connection Test (PING):", pingResult);

    const manager = new RedisScanManager({
      redis: redis,
      indexPrefix: "test_idx:",
      hashChars: 2
    });

    const remainingCount = await manager.count(dataPrefix);
    console.log(
      `Keys with prefix "${dataPrefix}" before cleanup: ${remainingCount}`
    );

    if (remainingCount === 0) {
      console.log("No keys to delete. Exiting.");
      redis.quit();
      return;
    }

    const deletedCount = await cleanupData(manager, dataPrefix);

    const finalCount = await manager.count(dataPrefix);
    console.log(
      `Keys with prefix "${dataPrefix}" after cleanup: ${finalCount}`
    );

    if (finalCount === 0) {
      console.log("✅ Cleanup completed successfully!");
    } else {
      console.error(`❌ Cleanup incomplete! ${finalCount} keys remaining.`);
    }
  } catch (error) {
    console.error("Cleanup failed:", error);
  } finally {
    console.log("Closing Redis connection...");
    redis.quit();
  }
}

runCleanup();
