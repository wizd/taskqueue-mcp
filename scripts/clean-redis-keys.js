#!/usr/bin/env node

/**
 * Redis键清理脚本
 * 用于清理多租户环境中的错误格式键
 */
const { Redis } = require('ioredis');
const readline = require('readline');

// 创建Redis客户端
const createRedisClient = (config = {}) => {
    const defaultConfig = {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379', 10),
        password: process.env.REDIS_PASSWORD,
        db: parseInt(process.env.REDIS_DB || '0', 10),
    };

    const redisConfig = { ...defaultConfig, ...config };
    return new Redis(redisConfig);
};

// 主函数
async function main() {
    const redis = createRedisClient();

    try {
        console.log('连接到Redis...');

        // 查找所有键
        console.log('扫描所有键...');
        const allKeys = await redis.keys('*');
        console.log(`找到 ${allKeys.length} 个键`);

        if (allKeys.length === 0) {
            console.log('没有键需要处理');
            return;
        }

        // 查找格式错误的键
        const problematicKeys = [];

        // 1. 带有前导冒号的键，如 `:tenant_deepchat_proj_proj-1:meta`
        const leadingColonKeys = allKeys.filter(key => key.startsWith(':'));
        problematicKeys.push(...leadingColonKeys);

        // 2. 使用bull:前缀但实际应该使用租户前缀的键
        const bullKeys = allKeys.filter(key => key.startsWith('bull:') && key.includes('tenant_'));
        problematicKeys.push(...bullKeys);

        // 3. 显示重复的键 (同一项目ID有多种不同前缀格式的键)
        const projectPattern = /proj-(\d+)/;
        const projectIdToKeys = {};

        allKeys.forEach(key => {
            const match = key.match(projectPattern);
            if (match && match[1]) {
                const projectId = match[1];
                if (!projectIdToKeys[projectId]) {
                    projectIdToKeys[projectId] = [];
                }
                projectIdToKeys[projectId].push(key);
            }
        });

        const duplicateProjectKeys = Object.entries(projectIdToKeys)
            .filter(([_, keys]) => keys.length > 1)
            .flatMap(([_, keys]) => keys);

        // 合并所有问题键到一个集合中去重
        const uniqueProblematicKeys = [...new Set([...problematicKeys, ...duplicateProjectKeys])];

        if (uniqueProblematicKeys.length === 0) {
            console.log('未发现格式错误的键，Redis数据正常');
            return;
        }

        console.log(`\n发现 ${uniqueProblematicKeys.length} 个格式错误的键:`);
        uniqueProblematicKeys.forEach((key, index) => {
            console.log(`${index + 1}. ${key}`);
        });

        // 询问用户是否删除这些键
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        const answer = await new Promise(resolve => {
            rl.question('\n是否删除这些格式错误的键? (yes/no): ', resolve);
        });

        if (answer.toLowerCase() === 'yes') {
            console.log('正在删除格式错误的键...');

            // 批量删除键
            if (uniqueProblematicKeys.length > 0) {
                const result = await redis.del(...uniqueProblematicKeys);
                console.log(`成功删除 ${result} 个键`);
            }

            console.log('清理完成');
        } else {
            console.log('操作已取消，未删除任何键');
        }

        rl.close();
    } catch (error) {
        console.error('执行过程中发生错误:', error);
    } finally {
        // 关闭Redis连接
        redis.disconnect();
    }
}

main().catch(console.error); 