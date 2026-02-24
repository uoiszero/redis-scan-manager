/**
 * 推导 Key 的字典序范围
 *
 * 用于 ZRANGEBYLEX 查询。根据 startKey 自动推导 endKey (如果未提供)。
 *
 * @param {string} startKey - 起始 Key (包含)
 * @param {string} [endKey] - 结束 Key (包含)
 * @returns {{lexStart: string, lexEnd: string}} Redis ZSET 字典序范围
 * @throws {Error} 如果无法根据 startKey 推导 endKey
 */
export function inferRange(
  startKey: string,
  endKey?: string
): { lexStart: string; lexEnd: string } {
  const lexStart = `[${startKey}`;
  let lexEnd: string;

  if (!endKey) {
    // 尝试根据分隔符推导前缀范围
    // 支持的分隔符: _ : - / #
    const match = startKey.match(/^([a-zA-Z0-9]+)(_|:|-|\/|#)/);
    if (match) {
      const prefix = match[0];
      const lastChar = prefix.slice(-1);
      // 最后一个字符 ASCII 码 + 1，即可涵盖该前缀下的所有可能的 Key
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
