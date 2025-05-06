#!/usr/bin/env node

/**
 * Redis键修复工具 - 增强版
 * 用于修复多租户环境中的所有键命名问题
 * 
 * 功能:
 * 1. 修复前导冒号键
 * 2. 修复BullMQ队列名
 * 3. 修复格式不一致的租户前缀
 * 
 * 使用方法:
 * node scripts/migration/fix-redis-keys.js [--all] [--tenant <tenant>] [--dry-run] [--redis-url <url>]
 */

const Redis = require('ioredis');
const { Command } = require('commander');
const readline = require('readline');

const program = new Command();
program
    .name('fix-redis-keys')
    .description('修复Redis中所有键命名问题')
    .option('-a, --all', '修复所有租户的键', false)
    .option('-t, --tenant <tenant>', '要处理的特定租户ID')
    .option('-d, --dry-run', '只显示将要修改的键，不实际修改', false)
    .option('-r, --redis-url <url>', 'Redis连接URL', 'redis://localhost:6379')
    .option('-f, --force', '跳过确认直接执行', false)
    .parse(process.argv);

const options = program.opts();

// 验证选项
if (!options.all && !options.tenant) {
    console.error('错误: 必须指定 --all 或者 --tenant <tenant>');
    program.help();
    process.exit(1);
}

// 正则表达式集合
const patterns = {
    // 前导冒号的错误格式
    leadingColon: /^:(.+)/,
    // 项目ID识别
    projectId: /proj-(\d+)/,
    // 租户ID识别 (从键名中)
    tenantFromKey: /tenant[_:]([^_:]+)[_:]/,
    // 不规范的队列前缀格式
    irregularQueuePrefix: /bull:tenant[_:]([^_:]+)[_:]/,
};

/**
 * 提取租户ID的函数
 * @param {string} key Redis键名
 * @returns {string|null} 租户ID或null
 */
function extractTenantId(key) {
    // 尝试从键名中提取租户ID
    const tenantMatch = key.match(patterns.tenantFromKey);
    return tenantMatch ? tenantMatch[1] : null;
}

/**
 * 生成正确格式的队列名
 * @param {string} tenantId 租户ID
 * @param {string} projectId 项目ID
 * @returns {string} 正确格式的队列名
 */
function generateCorrectQueueName(tenantId, projectId) {
    return tenantId
        ? `tenant_${tenantId}_proj_${projectId}`
        : `proj_${projectId}`;
}

/**
 * 生成正确格式的Redis键
 * @param {string} tenantId 租户ID
 * @param {string} key 原始键
 * @returns {string} 正确格式的键
 */
function generateCorrectKey(tenantId, key) {
    // 移除前导冒号
    let cleanKey = key.replace(/^:+/, '');

    // 处理特殊情况 - BullMQ队列键
    if (cleanKey.startsWith('bull:')) {
        // 如果是bull:tenant_xxx_proj_xxx格式
        if (cleanKey.match(/bull:tenant[_:]([^_:]+)[_:]proj[_:]/)) {
            // 去除bull:前缀
            cleanKey = cleanKey.replace(/^bull:/, '');
        }
        // 如果是bull:proj_xxx格式且有租户ID
        else if (tenantId && cleanKey.match(/bull:proj_/)) {
            // 提取projectId
            const projectMatch = cleanKey.match(/bull:proj_(proj-\d+)/);
            if (projectMatch && projectMatch[1]) {
                const projectId = projectMatch[1];
                // 使用租户ID和项目ID生成标准队列名
                const baseName = generateCorrectQueueName(tenantId, projectId);
                // 保留后缀 (meta, wait, events等)
                const suffix = cleanKey.includes(':')
                    ? cleanKey.substring(cleanKey.lastIndexOf(':'))
                    : '';
                cleanKey = baseName + suffix;
            }
        }
    }

    return cleanKey;
}

/**
 * 交互式确认
 * @param {string} message 确认消息
 * @returns {Promise<boolean>} 是否确认
 */
async function confirm(message) {
    if (options.force) return true;

    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise(resolve => {
        rl.question(`${message} (y/N): `, answer => {
            rl.close();
            resolve(answer.toLowerCase() === 'y');
        });
    });
}

async function main() {
    const redis = new Redis(options.redisUrl);
    console.log(`连接到Redis: ${options.redisUrl}`);

    try {
        // 1. 扫描所有键
        console.log('扫描所有键...');
        const allKeys = await redis.keys('*');
        console.log(`找到 ${allKeys.length} 个键`);

        if (allKeys.length === 0) {
            console.log('没有键需要处理');
            return;
        }

        // 2. 按租户ID分组
        const tenantGroups = {};
        const tenantsFound = new Set();
        const noTenantKeys = [];

        for (const key of allKeys) {
            const tenantId = extractTenantId(key);

            if (tenantId) {
                tenantsFound.add(tenantId);
                tenantGroups[tenantId] = tenantGroups[tenantId] || [];
                tenantGroups[tenantId].push(key);
            } else {
                noTenantKeys.push(key);
            }
        }

        console.log(`发现 ${tenantsFound.size} 个租户: ${Array.from(tenantsFound).join(', ')}`);
        console.log(`发现 ${noTenantKeys.length} 个不属于任何租户的键`);

        // 3. 确定要处理的租户
        let tenantsToProcess = [];

        if (options.all) {
            tenantsToProcess = Array.from(tenantsFound);
            console.log(`将处理所有 ${tenantsToProcess.length} 个租户的键`);
        } else if (options.tenant) {
            if (tenantsFound.has(options.tenant)) {
                tenantsToProcess = [options.tenant];
                console.log(`将只处理租户 ${options.tenant} 的键`);
            } else {
                console.log(`警告: 未找到租户 ${options.tenant} 的键`);
                if (await confirm('是否继续处理未分组的键?')) {
                    console.log('将处理未分组的键');
                } else {
                    console.log('操作已取消');
                    return;
                }
            }
        }

        // 4. 处理每个租户的键
        for (const tenantId of tenantsToProcess) {
            const keys = tenantGroups[tenantId] || [];
            console.log(`\n处理租户 ${tenantId} 的 ${keys.length} 个键...`);

            // 找出需要修复的键
            const keysToFix = [];

            for (const key of keys) {
                const correctKey = generateCorrectKey(tenantId, key);

                if (key !== correctKey) {
                    keysToFix.push({
                        original: key,
                        corrected: correctKey
                    });
                }
            }

            if (keysToFix.length === 0) {
                console.log(`✅ 租户 ${tenantId} 的所有键格式都是正确的`);
                continue;
            }

            console.log(`找到 ${keysToFix.length} 个需要修复的键:`);
            keysToFix.forEach((keyPair, index) => {
                console.log(`${index + 1}. ${keyPair.original} -> ${keyPair.corrected}`);
            });

            if (options.dryRun) {
                console.log('✅ 干运行模式，跳过实际修复');
                continue;
            }

            // 确认是否修复
            if (!await confirm(`是否修复这些键?`)) {
                console.log('已跳过修复');
                continue;
            }

            // 执行修复
            console.log('开始修复...');

            for (const { original, corrected } of keysToFix) {
                try {
                    // 获取键类型
                    const keyType = await redis.type(original);

                    // 根据类型迁移数据
                    switch (keyType) {
                        case 'string':
                            const value = await redis.get(original);
                            await redis.set(corrected, value);
                            break;

                        case 'hash':
                            const hash = await redis.hgetall(original);
                            if (Object.keys(hash).length > 0) {
                                await redis.hmset(corrected, hash);
                            }
                            break;

                        case 'list':
                            const list = await redis.lrange(original, 0, -1);
                            if (list.length > 0) {
                                await redis.lpush(corrected, ...list);
                            }
                            break;

                        case 'set':
                            const set = await redis.smembers(original);
                            if (set.length > 0) {
                                await redis.sadd(corrected, ...set);
                            }
                            break;

                        case 'zset':
                            const zset = await redis.zrange(original, 0, -1, 'WITHSCORES');
                            if (zset.length > 0) {
                                const args = [];
                                for (let i = 0; i < zset.length; i += 2) {
                                    args.push(zset[i + 1]); // score
                                    args.push(zset[i]);   // member
                                }
                                await redis.zadd(corrected, ...args);
                            }
                            break;

                        default:
                            console.warn(`无法处理类型为 ${keyType} 的键: ${original}`);
                            continue;
                    }

                    // 删除原来的键
                    await redis.del(original);
                    console.log(`已修复: ${original} -> ${corrected}`);

                } catch (error) {
                    console.error(`修复键 ${original} 时出错:`, error);
                }
            }

            console.log(`✅ 已完成租户 ${tenantId} 的键修复`);
        }

        // 5. 处理没有租户的键
        if (noTenantKeys.length > 0 && (options.all || !tenantsFound.has(options.tenant))) {
            console.log(`\n处理 ${noTenantKeys.length} 个不属于任何租户的键...`);

            // 找出需要修复的键
            const keysToFix = [];

            for (const key of noTenantKeys) {
                const correctKey = generateCorrectKey(null, key);

                if (key !== correctKey) {
                    keysToFix.push({
                        original: key,
                        corrected: correctKey
                    });
                }
            }

            if (keysToFix.length === 0) {
                console.log('✅ 所有非租户键格式都是正确的');
            } else {
                console.log(`找到 ${keysToFix.length} 个需要修复的键:`);
                keysToFix.forEach((keyPair, index) => {
                    console.log(`${index + 1}. ${keyPair.original} -> ${keyPair.corrected}`);
                });

                if (options.dryRun) {
                    console.log('✅ 干运行模式，跳过实际修复');
                } else if (await confirm(`是否修复这些键?`)) {
                    console.log('开始修复...');

                    for (const { original, corrected } of keysToFix) {
                        try {
                            // 获取键类型
                            const keyType = await redis.type(original);

                            // 根据类型迁移数据
                            switch (keyType) {
                                case 'string':
                                    const value = await redis.get(original);
                                    await redis.set(corrected, value);
                                    break;

                                case 'hash':
                                    const hash = await redis.hgetall(original);
                                    if (Object.keys(hash).length > 0) {
                                        await redis.hmset(corrected, hash);
                                    }
                                    break;

                                case 'list':
                                    const list = await redis.lrange(original, 0, -1);
                                    if (list.length > 0) {
                                        await redis.lpush(corrected, ...list);
                                    }
                                    break;

                                case 'set':
                                    const set = await redis.smembers(original);
                                    if (set.length > 0) {
                                        await redis.sadd(corrected, ...set);
                                    }
                                    break;

                                case 'zset':
                                    const zset = await redis.zrange(original, 0, -1, 'WITHSCORES');
                                    if (zset.length > 0) {
                                        const args = [];
                                        for (let i = 0; i < zset.length; i += 2) {
                                            args.push(zset[i + 1]); // score
                                            args.push(zset[i]);   // member
                                        }
                                        await redis.zadd(corrected, ...args);
                                    }
                                    break;

                                default:
                                    console.warn(`无法处理类型为 ${keyType} 的键: ${original}`);
                                    continue;
                            }

                            // 删除原来的键
                            await redis.del(original);
                            console.log(`已修复: ${original} -> ${corrected}`);

                        } catch (error) {
                            console.error(`修复键 ${original} 时出错:`, error);
                        }
                    }

                    console.log('✅ 已完成非租户键修复');
                } else {
                    console.log('已跳过修复');
                }
            }
        }

        console.log('\n✅ 所有键修复操作已完成');

    } catch (error) {
        console.error('处理Redis键时出错:', error);
        process.exit(1);
    } finally {
        redis.disconnect();
    }
}

main().catch(console.error); 