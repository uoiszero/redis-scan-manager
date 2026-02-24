import crypto from "crypto";

/**
 * 根据后缀获取桶的完整 Key
 *
 * @param {string} indexPrefix - 索引前缀
 * @param {string} suffix - 桶后缀
 * @returns {string} 完整桶 Key
 */
export function getBucketName(indexPrefix: string, suffix: string): string {
  return `${indexPrefix}${suffix}`;
}

/**
 * 计算 Key 所属的桶名
 *
 * 算法：MD5(key) -> Hex -> Take first N chars
 *
 * @param {string} key - 原始 Key
 * @param {string} indexPrefix - 索引前缀
 * @param {number} hashChars - 哈希位数
 * @returns {string} 桶的完整 Key
 */
export function getBucketKey(
  key: string,
  indexPrefix: string,
  hashChars: number
): string {
  const hash = crypto.createHash("md5").update(key).digest("hex");
  const bucketSuffix = hash.substring(0, hashChars);
  return getBucketName(indexPrefix, bucketSuffix);
}
