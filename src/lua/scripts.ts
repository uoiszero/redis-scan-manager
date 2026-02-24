/**
 * 原子添加数据和索引的 Lua 脚本
 *
 * KEYS[1]: Data Key (e.g., "user:1001")
 * KEYS[2]: Bucket Key (e.g., "idx:a5")
 * ARGV[1]: Data Value (JSON string)
 */
export const ADD_INDEX_SCRIPT = `
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('ZADD', KEYS[2], 0, KEYS[1])
`;

/**
 * 批量删除数据和索引的 Lua 脚本
 *
 * KEYS[1]: 这里的 KEYS 长度是动态的，每两个一组 (DataKey, BucketKey)
 *
 * 逻辑：
 * 遍历 KEYS 数组，每次取两个：
 *   KEYS[i]: Data Key
 *   KEYS[i+1]: Bucket Key
 * 分别执行 DEL 和 ZREM
 */
export const M_DEL_INDEX_SCRIPT = `
  for i=1, #KEYS, 2 do
    redis.call('DEL', KEYS[i])
    redis.call('ZREM', KEYS[i+1], KEYS[i])
  end
`;
