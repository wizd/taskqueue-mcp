import express from 'express';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
// @ts-ignore
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'; // 使用原始路径，并忽略 TS 错误
import { Queue, QueueEvents } from 'bullmq';
import { RedisManager } from './RedisManager.js';
import { RedisKeys } from '../types/bullmq.js';
import { AppError, AppErrorCode } from '../types/errors.js';

// 存储已添加到监控面板的队列适配器
const queueAdapters = new Map<string, BullMQAdapter>();
// 存储服务器适配器的引用
let serverAdapter: ExpressAdapter | null = null;
// 存储是否已初始化
let isInitialized = false;
// 存储bull-board API的方法引用
let bullBoardApi: { addQueue: (adapter: BullMQAdapter) => void } | null = null;
// Redis键的监听器
let keyspaceNotificationListener: QueueEvents | null = null;

/**
 * 创建新队列的适配器并添加到面板
 * @param projectId 项目ID
 * @param redis Redis连接
 * @returns 创建的适配器
 */
async function createAndAddQueueAdapter(projectId: string, redis: any): Promise<BullMQAdapter | null> {
  try {
    if (!serverAdapter || !isInitialized || !bullBoardApi) {
      console.warn(`Bull Board: 尝试为项目 ${projectId} 添加队列适配器，但面板尚未初始化`);
      return null;
    }

    // 如果已经存在该队列的适配器，则跳过
    if (queueAdapters.has(projectId)) {
      return queueAdapters.get(projectId) || null;
    }

    const queueName = RedisKeys.projectQueueName(projectId);
    console.log(`Bull Board: 为新发现的队列 ${queueName} 创建适配器`);

    const queue = new Queue(queueName, {
      connection: redis,
      // 如果有需要，可以在这里添加prefix
    });

    const adapter = new BullMQAdapter(queue);
    queueAdapters.set(projectId, adapter);

    // 将适配器添加到面板
    bullBoardApi.addQueue(adapter);
    console.log(`Bull Board: 成功添加队列 ${queueName} 到监控面板`);

    return adapter;
  } catch (error) {
    console.error(`Bull Board: 为项目 ${projectId} 添加队列适配器时出错:`, error);
    return null;
  }
}

/**
 * 监听新项目的创建并添加到监控面板
 * @param redis Redis连接
 */
async function setupQueueDiscovery(redis: any) {
  try {
    // 1. 设置Redis键空间通知（如果redis.config不支持，可能需要手动启用）
    try {
      // 尝试启用键空间通知（这需要redis配置允许）
      await redis.config('SET', 'notify-keyspace-events', 'KEA');
      console.log('Bull Board: 已启用Redis键空间通知');
    } catch (error) {
      console.warn('Bull Board: 无法启用Redis键空间通知，将使用定时扫描来发现新队列', error);
    }

    // 2. 设置定时扫描
    const scanInterval = 60000; // 每分钟扫描一次
    console.log(`Bull Board: 设置定时扫描，间隔${scanInterval}毫秒`);

    setInterval(async () => {
      try {
        // 扫描所有项目元数据键
        const keys = await redis.keys(RedisKeys.projectMetadataPattern());
        
        for (const key of keys) {
          const match = key.match(RedisKeys.projectIdFromKeyRegex());
          const projectId = match ? match[1] : null;
          
          if (projectId && !queueAdapters.has(projectId)) {
            await createAndAddQueueAdapter(projectId, redis);
          }
        }
      } catch (error) {
        console.error('Bull Board: 定时扫描项目队列时出错:', error);
      }
    }, scanInterval);

    // 3. 使用Redis PubSub来监听新项目创建
    // 本方案使用键空间通知，需要Redis服务器启用键空间通知功能
    try {
      const pubsub = redis.duplicate();
      await pubsub.subscribe('__keyspace@*__:*');
      
      pubsub.on('message', async (channel: string, message: string) => {
        // 键空间通知格式: __keyspace@<db>__:<key>
        if (message === 'hset' || message === 'set') {
          const key = channel.split(':').slice(1).join(':');
          if (key.match(RedisKeys.projectIdFromKeyRegex())) {
            const match = key.match(RedisKeys.projectIdFromKeyRegex());
            const projectId = match ? match[1] : null;
            
            if (projectId && !queueAdapters.has(projectId)) {
              await createAndAddQueueAdapter(projectId, redis);
            }
          }
        }
      });
      
      console.log('Bull Board: 已启用Redis键空间通知监听');
    } catch (error) {
      console.warn('Bull Board: 无法设置Redis键空间通知监听，将依赖定时扫描', error);
    }
  } catch (error) {
    console.error('Bull Board: 设置队列发现机制时出错:', error);
  }
}

/**
 * 启动 Bull Board UI 服务
 * @param port 监听端口，默认为 3000
 * @param basePath UI 的基础路径，默认为 /bull-board
 */
export async function startBullBoard(port = 3000, basePath = '/bull-board') {
  if (isInitialized) {
    console.warn('Bull Board: 已经初始化，忽略重复调用');
    return;
  }

  try {
    const app = express();
    serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath(basePath);

    // 初始化 Redis 连接
    const redisManager = RedisManager.getInstance();
    await redisManager.initialize(); // 确保 Redis 连接已建立
    const redis = redisManager.getConnection();

    // 1) 从 Redis 扫描所有项目 ID 的 metadata 键
    const keys = await redis.keys(RedisKeys.projectMetadataPattern());
    if (!keys || keys.length === 0) {
      console.warn('Bull Board: 未找到任何项目队列进行监控。');
    }

    const projectIds = keys.map(key => {
      const match = key.match(RedisKeys.projectIdFromKeyRegex());
      return match ? match[1] : null;
    }).filter(Boolean) as string[];

    console.log(`Bull Board: 初始发现 ${projectIds.length} 个项目队列: ${projectIds.join(', ')}`);

    // 2) 为每个项目队列创建一个 Adapter
    const queues = [];
    for (const id of projectIds) {
      const queueName = RedisKeys.projectQueueName(id);
      const queue = new Queue(queueName, {
        connection: redis,
      });
      const adapter = new BullMQAdapter(queue);
      queueAdapters.set(id, adapter);
      queues.push(adapter);
      console.log(`Bull Board: 为队列 ${queueName} 创建初始适配器`);
    }

    // 3) 挂载到 bull-board
    const { addQueue, removeQueue, setQueues } = createBullBoard({ 
      queues, 
      serverAdapter 
    });
    
    // 存储API引用以便后续使用
    bullBoardApi = { addQueue };

    // 4) 设置动态队列发现机制
    setupQueueDiscovery(redis);

    app.use(basePath, serverAdapter.getRouter());
    isInitialized = true;

    app.listen(port, () => {
      console.log(`Bull Board UI 运行在 http://localhost:${port}${basePath}`);
    });

  } catch (error) {
    console.error('启动 Bull Board UI 时出错:', error);
    // 重置状态以便重试
    isInitialized = false;
    serverAdapter = null;
    bullBoardApi = null;
    
    // 抛出 AppError 以便上层可以捕获
    throw new AppError(
      '启动 Bull Board UI 失败',
      AppErrorCode.Unknown,
      error
    );
  }
}

/**
 * 手动添加一个队列到监控面板
 * 当队列不是通过标准方式创建时可以调用此函数
 * @param projectId 项目ID
 */
export async function addQueueToBoard(projectId: string): Promise<void> {
  if (!isInitialized || !serverAdapter || !bullBoardApi) {
    throw new AppError(
      'Bull Board UI 尚未初始化，无法添加队列',
      AppErrorCode.ServiceNotReadyError
    );
  }
  
  try {
    const redisManager = RedisManager.getInstance();
    const redis = redisManager.getConnection();
    
    await createAndAddQueueAdapter(projectId, redis);
  } catch (error) {
    console.error(`Bull Board: 手动添加队列 ${projectId} 时出错:`, error);
    throw new AppError(
      `无法添加队列 ${projectId} 到监控面板`,
      AppErrorCode.Unknown,
      error
    );
  }
} 