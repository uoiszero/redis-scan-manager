import Redis from "ioredis";
import { RedisScanManager } from "../dist/index.js";
import assert from "assert/strict";

const config = {
  host: "localhost",
  port: 6379,
};

async function runFunctionalTest() {
  console.log("🚀 Starting Functional Test...");
  
  const redis = new Redis(config);
  const manager = new RedisScanManager({
    redis,
    indexPrefix: "func_idx:",
    hashChars: 1, // Use fewer buckets for small scale test
  });

  const PREFIX = "func_user:";
  
  try {
    // 0. Clean up previous test data
    console.log("🧹 Cleaning up previous data...");
    await manager.scan(PREFIX, undefined, 1000).then(async (results) => {
      const keys = [];
      for (let i = 0; i < results.length; i += 2) {
        keys.push(results[i]);
      }
      if (keys.length > 0) {
        await manager.del(keys);
      }
    });

    // 1. Test Add
    console.log("📝 Testing add()...");
    const users = [
      { id: "001", name: "Alice", role: "admin" },
      { id: "002", name: "Bob", role: "user" },
      { id: "003", name: "Charlie", role: "user" },
      { id: "004", name: "Dave", role: "guest" },
      { id: "005", name: "Eve", role: "admin" },
    ];

    for (const user of users) {
      await manager.add(`${PREFIX}${user.id}`, JSON.stringify(user));
    }
    console.log("✅ Added 5 users successfully.");

    // 2. Test Count
    console.log("🔢 Testing count()...");
    const count = await manager.count(PREFIX);
    assert.equal(count, 5, "Count should be 5");
    console.log(`✅ Count verified: ${count}`);

    // 3. Test Scan (All)
    console.log("🔍 Testing scan(all)...");
    const allUsers = await manager.scan(PREFIX, undefined, 10);
    assert.equal(allUsers.length, 10, "Should return 10 items (5 keys + 5 values)");
    
    // Verify content
    const firstKey = allUsers[0];
    const firstVal = JSON.parse(allUsers[1]);
    assert.equal(firstKey, `${PREFIX}001`, "First key should be 001");
    assert.equal(firstVal.name, "Alice", "First value should be Alice");
    console.log("✅ Scan all verified.");

    // 4. Test Scan (Limit & Range)
    console.log("🔍 Testing scan(limit & range)...");
    // Scan range 002 to 004
    const rangeUsers = await manager.scan(`${PREFIX}002`, `${PREFIX}004`, 10);
    assert.equal(rangeUsers.length, 6, "Should return 3 users (002, 003, 004)");
    assert.equal(rangeUsers[0], `${PREFIX}002`);
    assert.equal(rangeUsers[4], `${PREFIX}004`);
    console.log("✅ Range scan verified.");

    // 5. Test Del (Single)
    console.log("🗑️ Testing del(single)...");
    await manager.del(`${PREFIX}001`);
    const countAfterDel = await manager.count(PREFIX);
    assert.equal(countAfterDel, 4, "Count should be 4 after deleting 1");
    
    const checkDel = await manager.scan(`${PREFIX}001`, `${PREFIX}001`, 10);
    assert.equal(checkDel.length, 0, "Deleted key should not be found");
    console.log("✅ Single delete verified.");

    // 6. Test Del (Batch)
    console.log("🗑️ Testing del(batch)...");
    await manager.del([`${PREFIX}002`, `${PREFIX}003`]);
    const countAfterBatchDel = await manager.count(PREFIX);
    assert.equal(countAfterBatchDel, 2, "Count should be 2 after deleting 2 more");
    console.log("✅ Batch delete verified.");

    // 7. Cleanup remaining
    await manager.del([`${PREFIX}004`, `${PREFIX}005`]);
    const finalCount = await manager.count(PREFIX);
    assert.equal(finalCount, 0, "Final count should be 0");
    console.log("✅ Cleanup verified.");

    console.log("\n🎉 All functional tests passed!");

  } catch (err) {
    console.error("\n❌ Test Failed:", err);
    process.exit(1);
  } finally {
    redis.quit();
  }
}

runFunctionalTest();
