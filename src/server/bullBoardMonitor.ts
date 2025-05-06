import express from 'express';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
// @ts-ignore
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'; // 使用原始路径，并忽略 TS 错误
import { Queue } from 'bullmq';
import { RedisManager } from './RedisManager.js';
import { RedisKeys } from '../types/bullmq.js';
import { AppError, AppErrorCode } from '../types/errors.js';

/**
 * 启动 Bull Board UI 服务
 * @param port 监听端口，默认为 3000
 * @param basePath UI 的基础路径，默认为 /bull-board
 */
export async function startBullBoard(port = 3000, basePath = '/bull-board') {
  try {
    const app = express();
    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath(basePath);

    // 初始化 Redis 连接
    const redisManager = RedisManager.getInstance();
    await redisManager.initialize(); // 确保 Redis 连接已建立
    const redis = redisManager.getConnection();

    // 1) 从 Redis 扫描所有项目 ID 的 metadata 键
    // 注意：在生产环境中，如果项目非常多，KEYS 命令可能会阻塞 Redis。
    // 可以考虑使用 SCAN 或维护一个专门的项目 ID 列表。
    const keys = await redis.keys(RedisKeys.projectMetadataPattern()); // 使用 RedisKeys 中的模式
    if (!keys || keys.length === 0) {
        console.warn('Bull Board: 未找到任何项目队列进行监控。');
    }

    const projectIds = keys.map(key => {
        const match = key.match(RedisKeys.projectIdFromKeyRegex()); // 使用 RedisKeys 中的正则表达式
        return match ? match[1] : null;
    }).filter(Boolean) as string[];

    console.log(`Bull Board: 发现 ${projectIds.length} 个项目队列: ${projectIds.join(', ')}`);

    // 2) 为每个项目队列创建一个 Adapter
    const queues = projectIds.map(id => {
      const queueName = RedisKeys.projectQueueName(id);
      const queue = new Queue(queueName, {
        connection: redis,
        // 注意：这里可能需要传递 prefix 如果在 BullMQService 中设置了
        // prefix: RedisManager.getInstance().getPrefix(), // 假设 RedisManager 存储了 prefix
      });
      console.log(`Bull Board: 为队列 ${queueName} 创建 Adapter`);
      return new BullMQAdapter(queue);
    });

    // 3) 挂载到 bull-board
    createBullBoard({ queues, serverAdapter });

    app.use(basePath, serverAdapter.getRouter());

    app.listen(port, () => {
      console.log(`Bull Board UI 运行在 http://localhost:${port}${basePath}`);
    });

  } catch (error) {
    console.error('启动 Bull Board UI 时出错:', error);
    // 抛出 AppError 以便上层可以捕获
    throw new AppError(
      '启动 Bull Board UI 失败',
      AppErrorCode.Unknown, // 或者定义一个特定的错误码
      error
    );
  }
} 