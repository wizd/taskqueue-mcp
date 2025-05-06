#!/usr/bin/env node

/**
 * Redis键清理工具
 * 用于清理多租户环境中格式错误的Redis键
 * 
 * 使用方法:
 * node scripts/clean-redis-keys.js --tenant <tenantId> [--dry-run] [--redis-url <url>]
 */

const Redis = require('ioredis');
const { Command } = require('commander');

const program = new Command();
program
    .name('clean-redis-keys')
    .description('清理多租户环境中格式错误的Redis键')
    .option('-t, --tenant <tenant>', '要处理的租户ID')
    .option('-d, --dry-run', '只显示将要删除的键，不实际删除', false)
    .option('-r, --redis-url <url>', 'Redis连接URL', 'redis://localhost:6379')
    .option('-p, --pattern <pattern>', '要匹配的键模式', '*')
    .parse(process.argv);

const options = program.opts();

if (!options.tenant) {
    console.error('错误: 必须提供租户ID');
    program.help();
    process.exit(1);
}

async function main() {
    const redis = new Redis(options.redisUrl);
    console.log(`连接到Redis: ${options.redisUrl}`);

    // 标准化租户ID格式
    const tenantId = options.tenant;
    const correctPrefix = `tenant:${tenantId}:`;

    try {
      // 1. 查找前导冒号错误的键
      const invalidKeysPatterns = [
          // 前导冒号错误的键
          `:tenant_${tenantId}_*`,
          // BullMQ默认前缀的键（应该使用租户前缀）
          `bull:tenant_${tenantId}_*`,
          // 其他可能的错误格式...
      ];

      let keysToFix = [];

      for (const pattern of invalidKeysPatterns) {
          const keys = await redis.keys(pattern);
          keysToFix = [...keysToFix, ...keys];
      }

      if (keysToFix.length === 0) {
          console.log(`✅ 未发现租户 ${tenantId} 的错误格式键`);
      } else {
          console.log(`发现 ${keysToFix.length} 个错误格式的键:`);

        for (const key of keysToFix) {
            // 分析键并确定正确的格式
            let correctKey = '';

          if (key.startsWith(':tenant_')) {
              // 移除前导冒号
              correctKey = key.slice(1);
              console.log(`错误键: ${key} -> 正确键: ${correctKey}`);
          } else if (key.startsWith('bull:tenant_')) {
              // 去除bull:前缀，保留tenant_tenant_id部分
              correctKey = key.replace('bull:', '');
              console.log(`错误键: ${key} -> 正确键: ${correctKey}`);
          }

          if (!options.dryRun && correctKey) {
              try {
                  // 获取键类型
                  const keyType = await redis.type(key);

              // 根据键类型迁移数据
              switch (keyType) {
                  case 'string':
                      // 复制字符串值
                      const value = await redis.get(key);
                      await redis.set(correctKey, value);
                      break;
                  case 'hash':
                      // 复制哈希表
                      const hash = await redis.hgetall(key);
                      if (Object.keys(hash).length > 0) {
                          await redis.hmset(correctKey, hash);
                      }
                      break;
                  case 'list':
                      // 复制列表
                      const list = await redis.lrange(key, 0, -1);
                      if (list.length > 0) {
                          await redis.lpush(correctKey, ...list);
                      }
                      break;
                  case 'set':
                      // 复制集合
                      const set = await redis.smembers(key);
                      if (set.length > 0) {
                          await redis.sadd(correctKey, ...set);
                      }
                      break;
                  case 'zset':
                      // 复制有序集合
                      const zset = await redis.zrange(key, 0, -1, 'WITHSCORES');
                      if (zset.length > 0) {
                          const args = [];
                          for (let i = 0; i < zset.length; i += 2) {
                              args.push(zset[i + 1]); // score
                              args.push(zset[i]);   // member
                          }
                          await redis.zadd(correctKey, ...args);
                }
                    break;
                default:
                    console.warn(`无法处理类型为 ${keyType} 的键: ${key}`);
                    continue;
            }

              // 删除旧键
              await redis.del(key);
              console.log(`已成功迁移并删除键: ${key}`);
          } catch (error) {
              console.error(`处理键 ${key} 时出错:`, error);
          }
        }
        }

        if (options.dryRun) {
            console.log('✅ 干运行完成，未执行实际删除');
        } else {
              console.log('✅ 已完成键修复');
          }
      }

      // 2. 检查键命名一致性（可选）
      // 查找具有正确租户前缀的键
      const correctKeys = await redis.keys(`${correctPrefix}*`);
      console.log(`\n当前租户 ${tenantId} 有 ${correctKeys.length} 个正确格式的键`);

  } catch (error) {
      console.error('处理Redis键时出错:', error);
      process.exit(1);
  } finally {
      redis.disconnect();
  }
}

main().catch(console.error); 