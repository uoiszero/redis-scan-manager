import Redis from "ioredis";
import { RedisScanManager } from "../dist/index.js";

const singletonConfig = {
  host: "localhost",
  port: 6379
};

// const config = {
//   host: "localhost",
//   port: 6379
// };

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

const mode = process.env.TEST_MODE || "singleton";
console.log(`Running test in ${mode} mode...`);
const config = mode === "cluster" ? clusterConfig : singletonConfig;

/**
 * 测试批量写入数据
 *
 * 按照 BATCH_SIZE (默认500) 进行并发写入，以提高写入效率。
 * Key 格式为 `{dataPrefix}{00000-N}`，Value 为包含 id, name, timestamp 的 JSON 字符串。
 *
 * @param {RedisScanManager} manager - RedisScanManager 实例
 * @param {string} dataPrefix - 测试数据的 Key 前缀 (如 "test_user:")
 * @param {number} writeCount - 写入的数据总量
 * @returns {Promise<void>}
 */
async function testWriteData(manager, dataPrefix, writeCount) {
  console.log(`Writing ${writeCount} items...`);
  const startTime = Date.now();

  const BATCH_SIZE = 500;

  // Process in batches
  for (let i = 0; i < writeCount; i += BATCH_SIZE) {
    const batchStartTime = Date.now();
    const batchPromises = [];
    for (let j = 0; j < BATCH_SIZE && i + j < writeCount; j++) {
      const index = i + j;
      // Pad number for lexicographical order: 00000, 00001, ... 09999
      const id = index.toString().padStart(5, "0");
      const key = `${dataPrefix}${id}`;
      const value = JSON.stringify({
        id,
        name: `User ${id}`,
        timestamp: Date.now()
      });
      batchPromises.push(manager.add(key, value));
    }
    await Promise.all(batchPromises);

    const batchDuration = Date.now() - batchStartTime;
    if (i % 100000 === 0) {
      console.log(
        `  Batch ${i / BATCH_SIZE}: ${batchDuration}ms for ${BATCH_SIZE} items`
      );
    }
  }

  const duration = Date.now() - startTime;
  console.log(`Write completed in ${duration}ms`);
}

/**
 * 测试数据统计与范围查询
 *
 * 1. 使用 count() 方法统计总数，验证与写入数量是否一致。
 * 2. 使用 scan() 方法获取前 10 条数据，验证返回的数据结构与顺序是否正确。
 *
 * @param {RedisScanManager} manager - RedisScanManager 实例
 * @param {string} dataPrefix - 测试数据的 Key 前缀
 * @param {number} expectedCount - 期望的数据总量
 * @returns {Promise<void>}
 */
async function testScanData(manager, dataPrefix, expectedCount) {
  console.log("Counting data...");
  // Note: The count method expects a range.
  // If we want to count all keys starting with DATA_PREFIX, we need to handle the end key carefully.
  // The manager.count() internally infers endKey if not provided, similar to scan.
  const count = await manager.count(dataPrefix);
  console.log(`Total items count (via ZLEXCOUNT): ${count}`);

  // Verify count matches
  if (count === expectedCount) {
    console.log("✅ Count matches expected value.");
  } else {
    console.warn(`⚠️ Count mismatch! Expected ${expectedCount}, got ${count}`);
  }

  // 5. Scan Data
  console.log("Scanning data (limit 10)...");
  const scanResult = await manager.scan(dataPrefix, undefined, 10);

  console.log(`Scan returned ${scanResult.length / 2} items:`);
  for (let i = 0; i < scanResult.length; i += 2) {
    console.log(`  ${scanResult[i]}: ${scanResult[i + 1]}`);
  }

  // Check if data is correct
  if (scanResult.length > 0) {
    console.log("✅ Scan test passed.");
  } else {
    console.error("❌ Scan returned no data!");
  }
}

/**
 * 测试部分删除和重新写入
 *
 * 1. Scan 获取一半数据。
 * 2. 删除这部分数据。
 * 3. 验证剩余数量是否正确。
 * 4. 写入 123 条新数据。
 * 5. 验证总数量是否正确。
 *
 * @param {RedisScanManager} manager
 * @param {string} dataPrefix
 * @param {number} totalCount
 */
async function testPartialDeleteAndRewrite(manager, dataPrefix, totalCount) {
  console.log("\n=== Testing Partial Delete and Rewrite ===");
  const deleteCount = Math.floor(totalCount / 2);
  console.log(`Scanning first ${deleteCount} items to delete...`);

  // 由于 scan 的 limit 参数限制在 1-1000，我们需要分批获取并删除
  console.log(`Scanning and deleting first ${deleteCount} items in batches...`);

  let deletedSoFar = 0;
  const SCAN_LIMIT = 1000;

  while (deletedSoFar < deleteCount) {
    // 计算本次需要获取的数量
    const limit = Math.min(SCAN_LIMIT, deleteCount - deletedSoFar);

    // 每次都从头开始 scan，因为之前的数据已经被删除了
    // 这样可以确保每次取到的都是当前剩余数据的“前N个”
    const scanResult = await manager.scan(dataPrefix, undefined, limit);

    if (scanResult.length === 0) {
      console.warn("Scan returned no data, but delete count not reached.");
      break;
    }

    const keysToDelete = [];
    for (let i = 0; i < scanResult.length; i += 2) {
      keysToDelete.push(scanResult[i]);
    }

    await manager.del(keysToDelete);
    deletedSoFar += keysToDelete.length;

    if (deletedSoFar % 50000 === 0) {
      console.log(`  Deleted ${deletedSoFar}/${deleteCount}...`);
    }
  }

  // 验证剩余数量
  const remainingCount = await manager.count(dataPrefix);
  const expectedRemaining = totalCount - deleteCount;
  console.log(
    `Remaining count: ${remainingCount}, Expected: ${expectedRemaining}`
  );

  if (remainingCount === expectedRemaining) {
    console.log("✅ Partial delete verification passed.");
  } else {
    console.error(`❌ Partial delete verification failed!`);
  }

  // 写入 123 条新数据
  const newCount = 123;
  console.log(`Writing ${newCount} new items...`);
  const newItems = [];
  for (let i = 0; i < newCount; i++) {
    const id = `new_${i.toString().padStart(5, "0")}`;
    const key = `${dataPrefix}${id}`;
    const value = JSON.stringify({
      id,
      name: `New User ${id}`,
      timestamp: Date.now()
    });
    newItems.push({ key, value });
  }

  // 并发写入
  await Promise.all(newItems.map(item => manager.add(item.key, item.value)));

  // 验证最终数量
  const finalCount = await manager.count(dataPrefix);
  const expectedFinal = expectedRemaining + newCount;
  console.log(`Final count: ${finalCount}, Expected: ${expectedFinal}`);

  if (finalCount === expectedFinal) {
    console.log("✅ Rewrite verification passed.");
  } else {
    console.error(`❌ Rewrite verification failed!`);
  }
  console.log("=== Partial Delete and Rewrite Test Completed ===\n");
}

/**
 * 清理测试数据
 *
 * 采用流式清理策略 (Streaming Deletion)：
 * 1. 使用 scan() 方法分批次 (Batch=500) 获取 Key。
 * 2. 获取一批后立即执行删除，释放内存。
 * 3. 利用分页游标 (Next Key) 确保不重不漏。
 * 4. 清理完成后通过 count() 再次验证。
 *
 * @param {RedisScanManager} manager - RedisScanManager 实例
 * @param {string} dataPrefix - 待清理数据的 Key 前缀
 * @returns {Promise<void>}
 */
async function cleanupData(manager, dataPrefix) {
  // Use loop to fetch and delete keys in batches (Streaming Deletion)
  console.log("Cleaning up test data (Streaming Mode)...");

  let lastKey = undefined;
  let totalDeleted = 0;

  // Safety limit to prevent infinite loops
  let loopCount = 0;
  const MAX_LOOPS = 5000; // Increase max loops as we might process many batches

  while (loopCount < MAX_LOOPS) {
    loopCount++;

    let startKey = lastKey ? lastKey : dataPrefix;
    // If we have a lastKey, we append \x00 to it to start searching strictly after it
    if (lastKey) {
      startKey = lastKey + "\x00";
    }

    // Scan with limit (e.g. 500), keysOnly=true
    const batchData = await manager.scan(startKey, undefined, 500, true);

    if (batchData.length === 0) {
      break;
    }

    const batchKeysToDelete = [];
    for (let i = 0; i < batchData.length; i += 2) {
      batchKeysToDelete.push(batchData[i]);
    }

    // Save the last key for next iteration pagination
    const nextStartKey = batchData[batchData.length - 2];

    // Delete current batch immediately
    await manager.del(batchKeysToDelete);

    // Update lastKey AFTER deleting, but using the saved key from before deletion
    // Note: Even if we delete the key, lastKey is just a string used for lexicographical comparison in scan
    // so it doesn't matter if the key itself exists or not.
    lastKey = nextStartKey;

    totalDeleted += batchKeysToDelete.length;

    // If we fetched less than limit (500 items * 2 = 1000), it means we reached the end
    if (batchData.length < 1000) {
      break;
    }
  }

  console.log(`Cleanup finished. Total deleted: ${totalDeleted} keys.`);

  // 7. Verify Cleanup
  console.log("Verifying cleanup...");
  const remainingCount = await manager.count(dataPrefix);
  if (remainingCount === 0) {
    console.log("✅ All test data successfully deleted.");
  } else {
    console.error(`❌ Cleanup failed! ${remainingCount} items remaining.`);
  }
}

/**
 * 主测试流程
 *
 * 1. 连接 Redis (支持 Cluster/Standalone)。
 * 2. 写入 10000 条测试数据。
 * 3. 验证数据统计与范围查询功能。
 * 4. 清理所有测试数据。
 *
 * @returns {Promise<void>}
 */
async function runTest() {
  console.log("Starting RedisScanManager test...");

  // 1. Initialize Redis Client
  let redis;
  const redisOptions = {
    connectTimeout: 5000, // 5s timeout
    maxRetriesPerRequest: 1,
    enableReadyCheck: false,
    retryStrategy: times => {
      if (times > 3) return null; // Stop retrying after 3 attempts
      return Math.min(times * 100, 2000);
    }
  };

  if (Array.isArray(config)) {
    console.log("Config is an array, initializing Redis Cluster...");
    redis = new Redis.Cluster(config, {
      redisOptions: redisOptions,
      clusterRetryStrategy: times => {
        if (times > 3) return null;
        return Math.min(times * 100, 2000);
      }
    });
  } else {
    console.log("Config is an object, initializing Redis Standalone...");
    redis = new Redis({ ...config, ...redisOptions });
  }

  // Handle connection errors
  redis.on("error", err => {
    console.error("Redis Client Error:", err);
  });

  // Wait for connection to be ready
  console.log("Waiting for Redis connection...");
  await new Promise((resolve, reject) => {
    redis.once("ready", () => {
      console.log("Redis client is ready.");
      resolve();
    });

    // Fail fast if connection cannot be established within timeout
    setTimeout(() => {
      if (redis.status !== "ready") {
        reject(new Error("Redis connection timeout"));
      }
    }, 10000);
  });

  try {
    // Test Connection
    const pingResult = await redis.ping();
    console.log("Redis Connection Test (PING):", pingResult);

    // 2. Initialize RedisScanManager
    const manager = new RedisScanManager({
      redis: redis,
      indexPrefix: "test_idx:", // Use a test prefix for index
      hashChars: 2
    });

    const DATA_PREFIX = "scan_test_user:";
    const WRITE_COUNT = 100000;

    // 3. Write Data
    console.time("Write Data");
    await testWriteData(manager, DATA_PREFIX, WRITE_COUNT);
    console.timeEnd("Write Data");

    // 调试：第一次写入后
    const stats1 = await manager.getDebugStats();

    // 4. Scan and Verify Data
    console.time("Scan and Verify Data");
    await testScanData(manager, DATA_PREFIX, WRITE_COUNT);
    console.timeEnd("Scan and Verify Data");

    // 5. Test Partial Delete and Rewrite
    // 为了不影响 cleanupData，我们在这个函数内部完成自己的清理或者在 cleanupData 中适配
    // 但 cleanupData 是全量清理，所以这里可以直接调用，不影响后续清理逻辑
    console.time("Partial Delete and Rewrite");
    await testPartialDeleteAndRewrite(manager, DATA_PREFIX, WRITE_COUNT);
    console.timeEnd("Partial Delete and Rewrite");

    // 调试：删除一半并重写后
    const stats2 = await manager.getDebugStats();

    // 6. Cleanup Data
    console.time("Cleanup Data");
    await cleanupData(manager, DATA_PREFIX);
    console.timeEnd("Cleanup Data");

    // 输出最终统计结果
    console.log("\n\n#############################################");
    console.log("#            Benchmark Report               #");
    console.log("#############################################\n");

    console.log("--- 1. Initial State (After writing 1,000,000 items) ---");
    console.log(`Total Items: ${stats1.stats.totalItems}`);
    console.log(`Avg Bucket Size: ${stats1.stats.avgItems}`);
    console.log(
      `Max Bucket: ${stats1.stats.maxItems} (suffix: ${stats1.outliers.maxBucket.suffix})`
    );
    console.log(
      `Min Bucket: ${stats1.stats.minItems} (suffix: ${stats1.outliers.minBucket.suffix})`
    );
    console.log(
      `Skew Ratio: ${(stats1.stats.maxItems / stats1.stats.avgItems).toFixed(2)}`
    );

    console.log(
      "\n--- 2. After Partial Delete & Rewrite (Delete 500k, Add 123) ---"
    );
    console.log(`Total Items: ${stats2.stats.totalItems}`);
    console.log(`Avg Bucket Size: ${stats2.stats.avgItems}`);
    console.log(
      `Max Bucket: ${stats2.stats.maxItems} (suffix: ${stats2.outliers.maxBucket.suffix})`
    );
    console.log(
      `Min Bucket: ${stats2.stats.minItems} (suffix: ${stats2.outliers.minBucket.suffix})`
    );
    console.log(
      `Skew Ratio: ${(stats2.stats.maxItems / stats2.stats.avgItems).toFixed(2)}`
    );

    console.log("\n---------------------------------------------");
    console.log("Analysis:");
    const diff = stats2.stats.totalItems - stats1.stats.totalItems;
    const expected = (WRITE_COUNT/2)*-1 + 123;
    console.log(
      `Net Change: ${diff} items (Expected: -${WRITE_COUNT/2} + 123 = ${expected} items)`
    );
    if (diff === expected) {  
      console.log("✅ Data consistency verified.");
    } else {
      console.error("❌ Data consistency mismatch!");
    }
    console.log("#############################################\n");
  } catch (error) {
    console.error("Test failed:", error);
  } finally {
    // 6. Cleanup and Disconnect
    console.log("Closing Redis connection...");
    redis.quit();
  }
}

runTest();
