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
// 存储队列名到项目ID的映射
const queueNameToProjectId = new Map<string, string>();
// 存储发现的但尚未添加的队列，格式: [队列名, 项目ID, 队列选项]
const pendingQueues: Array<[string, string, any]> = [];
// 存储服务器适配器的引用
let serverAdapter: ExpressAdapter | null = null;
// 存储是否已初始化
let isInitialized = false;
// 存储bull-board API的方法引用
let bullBoardApi: { 
  addQueue: (adapter: BullMQAdapter) => void;
  removeQueue: (adapter: BullMQAdapter) => void;
} | null = null;
// Redis键的监听器
let keyspaceNotificationListener: QueueEvents | null = null;

/**
 * 从队列名称中提取项目ID
 * 支持多种格式: proj_proj-123, tenant:xxx::proj_proj-123
 * @param queueFullName 完整的队列名称
 * @returns 项目ID
 */
function extractProjectIdFromQueueName(queueFullName: string): string | null {
  // 1. 检查是否是有租户前缀的队列名
  const tenantPattern = /^tenant:.*?::proj_(proj-\d+)/;
  const tenantMatch = queueFullName.match(tenantPattern);
  if (tenantMatch && tenantMatch[1]) {
    return tenantMatch[1];
  }
  
  // 2. 检查标准队列名格式
  const standardPattern = /^proj_(proj-\d+)/;
  const standardMatch = queueFullName.match(standardPattern);
  if (standardMatch && standardMatch[1]) {
    return standardMatch[1];
  }
  
  // 3. 如果项目名称不符合预期格式，尝试从 bull: 前缀提取
  const bullPattern = /^bull:proj_(proj-\d+)/;
  const bullMatch = queueFullName.match(bullPattern);
  if (bullMatch && bullMatch[1]) {
    return bullMatch[1];
  }
  
  // 4. 自定义项目ID格式
  // 可以在这里添加其他格式的匹配规则
  
  return null; // 无法提取项目ID
}

/**
 * 创建队列名的标准化名称
 * 移除租户前缀和bull前缀，保留核心队列名
 * @param queueFullName 完整的队列名称
 * @returns 标准化的队列名
 */
function normalizeQueueName(queueFullName: string): string {
  // 1. 移除租户前缀
  const tenantPattern = /^tenant:.*?::(.*)/;
  const tenantMatch = queueFullName.match(tenantPattern);
  if (tenantMatch && tenantMatch[1]) {
    return tenantMatch[1];
  }
  
  // 2. 移除bull前缀
  const bullPattern = /^bull:(.*)/;
  const bullMatch = queueFullName.match(bullPattern);
  if (bullMatch && bullMatch[1]) {
    return bullMatch[1];
  }
  
  return queueFullName;
}

/**
 * 创建新队列的适配器并添加到面板
 * @param queueFullName 完整的队列名称 (可能包含租户前缀)
 * @param redis Redis连接
 * @param skipIfNotInitialized 如果面板未初始化，是否将队列加入待处理列表
 * @returns 创建的适配器
 */
async function createAndAddQueueAdapter(
  queueFullName: string, 
  redis: any, 
  skipIfNotInitialized: boolean = false
): Promise<BullMQAdapter | null> {
  try {
    // 提取项目ID，用作注册键
    const projectId = extractProjectIdFromQueueName(queueFullName);
    if (!projectId) {
      console.warn(`Bull Board: 无法从队列名 ${queueFullName} 提取项目ID，跳过`);
      return null;
    }
    
    // 标准化队列名 (移除前缀)
    const normalizedQueueName = normalizeQueueName(queueFullName);
    
    // 检查队列是否已注册
    if (queueAdapters.has(projectId)) {
      return queueAdapters.get(projectId) || null;
    }

    // 准备队列选项
    let queueOptions: any = {
      connection: redis,
    };
    
    // 如果是租户格式，从队列名称中提取前缀
    const tenantPrefix = queueFullName.match(/^tenant:(.*?)::/);
    if (tenantPrefix && tenantPrefix[1]) {
      queueOptions.prefix = `tenant:${tenantPrefix[1]}:`;
    } else if (queueFullName.startsWith('bull:')) {
      // 使用标准bull前缀
      queueOptions.prefix = 'bull:';
    }

    // 如果面板尚未初始化，但要求将队列添加到待处理列表
    if ((!serverAdapter || !isInitialized || !bullBoardApi) && skipIfNotInitialized) {
      // 将队列信息添加到待处理列表，等待初始化完成后处理
      pendingQueues.push([normalizedQueueName, projectId, queueOptions]);
      console.log(`Bull Board: 队列 ${queueFullName} 添加到待处理列表，等待面板初始化完成`);
      return null;
    }
    
    // 如果面板尚未初始化，且不要求添加到待处理列表，直接返回
    if (!serverAdapter || !isInitialized || !bullBoardApi) {
      console.warn(`Bull Board: 尝试为队列 ${queueFullName} 添加适配器，但面板尚未初始化`);
      return null;
    }
    
    console.log(`Bull Board: 为队列 ${queueFullName} 创建适配器，标准化名称: ${normalizedQueueName}`);

    const queue = new Queue(normalizedQueueName, queueOptions);
    const adapter = new BullMQAdapter(queue);
    
    // 使用项目ID作为键存储适配器引用
    queueAdapters.set(projectId, adapter);
    // 存储队列名到项目ID的映射，用于后续删除操作
    queueNameToProjectId.set(normalizedQueueName, projectId);

    // 将适配器添加到面板
    bullBoardApi.addQueue(adapter);
    console.log(`Bull Board: 成功添加队列 ${queueFullName} 到监控面板, 项目ID: ${projectId}`);

    return adapter;
  } catch (error) {
    console.error(`Bull Board: 为队列 ${queueFullName} 添加适配器时出错:`, error);
    return null;
  }
}

/**
 * 处理所有待处理的队列
 * 当面板初始化完成后调用
 */
async function processPendingQueues(redis: any): Promise<number> {
  let processed = 0;
  if (pendingQueues.length > 0) {
    console.log(`Bull Board: 开始处理 ${pendingQueues.length} 个待处理队列`);
    
    while (pendingQueues.length > 0) {
      const [normalizedQueueName, projectId, queueOptions] = pendingQueues.shift()!;
      
      try {
        if (!queueAdapters.has(projectId)) {
          const queue = new Queue(normalizedQueueName, queueOptions);
          const adapter = new BullMQAdapter(queue);
          
          queueAdapters.set(projectId, adapter);
          queueNameToProjectId.set(normalizedQueueName, projectId);
          
          bullBoardApi!.addQueue(adapter);
          processed++;
          
          console.log(`Bull Board: 成功添加待处理队列 ${normalizedQueueName} 到监控面板, 项目ID: ${projectId}`);
        }
      } catch (error) {
        console.error(`Bull Board: 处理待处理队列 ${normalizedQueueName} 时出错:`, error);
      }
    }
  }
  return processed;
}

/**
 * 扫描发现所有项目队列
 * 结合多种模式扫描所有队列
 * @param redis Redis连接
 * @param skipIfNotInitialized 如果面板未初始化，是否将队列加入待处理列表
 * @returns 发现的队列数量
 */
async function discoverAllQueues(redis: any, skipIfNotInitialized: boolean = false): Promise<number> {
  try {
    // 添加的队列计数
    let addedCount = 0;
    let discoveredCount = 0;
    
    // 1. 首先扫描标准模式的项目元数据
    const standardKeys = await redis.keys(RedisKeys.projectMetadataPattern());
    if (standardKeys && standardKeys.length > 0) {
      console.log(`Bull Board: 发现 ${standardKeys.length} 个标准项目元数据`);
      discoveredCount += standardKeys.length;
      
      for (const key of standardKeys) {
        const match = key.match(RedisKeys.projectIdFromKeyRegex());
        const projectId = match ? match[1] : null;
        
        if (projectId) {
          const queueName = RedisKeys.projectQueueName(projectId);
          const adapter = await createAndAddQueueAdapter(queueName, redis, skipIfNotInitialized);
          if (adapter) addedCount++;
        }
      }
    }
    
    // 2. 扫描bull:队列前缀的队列
    const bullQueuePattern = 'bull:proj_proj-*:meta';
    const bullMQQueues = await redis.keys(bullQueuePattern);
    if (bullMQQueues && bullMQQueues.length > 0) {
      console.log(`Bull Board: 发现 ${bullMQQueues.length} 个BullMQ队列元数据`);
      discoveredCount += bullMQQueues.length;
      
      for (const key of bullMQQueues) {
        // 从元数据键中提取队列名
        // 例如 bull:proj_proj-1:meta -> bull:proj_proj-1
        const queueName = key.replace(':meta', '');
        const projectId = extractProjectIdFromQueueName(queueName);
        
        if (projectId) {
          const adapter = await createAndAddQueueAdapter(queueName, redis, skipIfNotInitialized);
          if (adapter) addedCount++;
        }
      }
    }
    
    // 3. 扫描多租户格式的队列
    const tenantPatterns = [
      'tenant:*::proj_proj-*:meta',
      'tenant:*::proj_proj-*:wait',
      'tenant:*::proj_proj-*:events',
    ];
    
    for (const pattern of tenantPatterns) {
      const tenantQueues = await redis.keys(pattern);
      if (tenantQueues && tenantQueues.length > 0) {
        console.log(`Bull Board: 发现 ${tenantQueues.length} 个多租户队列 (模式: ${pattern})`);
        discoveredCount += tenantQueues.length;
        
        for (const key of tenantQueues) {
          // 提取不带后缀的队列名
          // 例如 tenant:someid::proj_proj-1:meta -> tenant:someid::proj_proj-1
          const suffixes = [':meta', ':wait', ':events'];
          let queueName = key;
          
          for (const suffix of suffixes) {
            queueName = queueName.replace(suffix, '');
          }
          
          const projectId = extractProjectIdFromQueueName(queueName);
          
          if (projectId) {
            const adapter = await createAndAddQueueAdapter(queueName, redis, skipIfNotInitialized);
            if (adapter) addedCount++;
          }
        }
      }
    }
    
    console.log(`Bull Board: 扫描发现了 ${discoveredCount} 个队列，成功添加了 ${addedCount} 个，还有 ${pendingQueues.length} 个待处理`);
    return addedCount;
  } catch (error) {
    console.error('Bull Board: 扫描队列时出错:', error);
    return 0;
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
        // 使用增强的队列发现函数
        const newQueuesCount = await discoverAllQueues(redis);
        if (newQueuesCount > 0) {
          console.log(`Bull Board: 定时扫描发现并添加了 ${newQueuesCount} 个新队列`);
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
          
          // 检查是否是元数据键模式
          if (key.match(/^(project:proj-\d+:metadata|bull:proj_proj-\d+:meta|tenant:.*?::proj_proj-\d+:meta)$/)) {
            // 提取队列名
            let queueName = '';
            
            if (key.startsWith('project:')) {
              // 从标准项目元数据中提取队列名
              const match = key.match(/^project:(proj-\d+):metadata$/);
              if (match && match[1]) {
                queueName = RedisKeys.projectQueueName(match[1]);
              }
            } else if (key.startsWith('bull:')) {
              // 从BullMQ键中提取队列名
              queueName = key.replace(':meta', '');
            } else if (key.startsWith('tenant:')) {
              // 从多租户键中提取队列名
              queueName = key.replace(':meta', '');
            }
            
            if (queueName) {
              const projectId = extractProjectIdFromQueueName(queueName);
              if (projectId && !queueAdapters.has(projectId)) {
                await createAndAddQueueAdapter(queueName, redis);
              }
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
    console.log(`尝试在端口 ${port} 和路径 ${basePath} 启动 Bull Board UI...`);
    const app = express();
    serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath(basePath);

    // 初始化 Redis 连接
    const redisManager = RedisManager.getInstance();
    await redisManager.initialize(); // 确保 Redis 连接已建立
    const redis = redisManager.getConnection();

    // 前置扫描 - 只是为了发现队列并添加到待处理列表，不实际添加
    await discoverAllQueues(redis, true);
    console.log(`Bull Board: 预扫描完成，有 ${pendingQueues.length} 个队列待处理`);

    // 先创建一个空的 Bull Board
    const { addQueue, removeQueue, setQueues } = createBullBoard({ 
      queues: [], 
      serverAdapter 
    });
    
    // 存储API引用以便后续使用
    bullBoardApi = { addQueue, removeQueue };
    isInitialized = true;

    // 现在 Bull Board 已初始化，处理待处理的队列
    const processedCount = await processPendingQueues(redis);
    console.log(`Bull Board: 已处理 ${processedCount} 个待处理队列`);

    // 最后再执行一次完整扫描，以防有遗漏
    const addedCount = await discoverAllQueues(redis);
    console.log(`Bull Board: 最终扫描添加了 ${addedCount} 个队列`);

    // 设置动态队列发现机制
    setupQueueDiscovery(redis);

    app.use(basePath, serverAdapter.getRouter());

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
    
    // 构造标准队列名
    const queueName = RedisKeys.projectQueueName(projectId);
    await createAndAddQueueAdapter(queueName, redis);
  } catch (error) {
    console.error(`Bull Board: 手动添加队列 ${projectId} 时出错:`, error);
    throw new AppError(
      `无法添加队列 ${projectId} 到监控面板`,
      AppErrorCode.Unknown,
      error
    );
  }
}

/**
 * 从监控面板中移除一个队列
 * 当项目被删除时应该调用此函数
 * @param projectId 项目ID
 */
export async function removeQueueFromBoard(projectId: string): Promise<void> {
  if (!isInitialized || !serverAdapter || !bullBoardApi) {
    // 如果 Bull Board 未初始化，则忽略请求（不抛出错误）
    console.warn(`Bull Board: 尝试移除队列 ${projectId}，但面板尚未初始化，忽略请求`);
    return;
  }
  
  try {
    // 检查队列适配器是否存在
    const adapter = queueAdapters.get(projectId);
    if (!adapter) {
      console.warn(`Bull Board: 队列 ${projectId} 不存在于监控面板中，忽略移除请求`);
      return;
    }
    
    // 从 Bull Board 中移除队列
    bullBoardApi.removeQueue(adapter);
    
    // 从适配器映射中移除
    queueAdapters.delete(projectId);
    
    // 从队列名到项目ID的映射中移除
    // 遍历映射查找对应的队列名
    for (const [queueName, id] of queueNameToProjectId.entries()) {
      if (id === projectId) {
        queueNameToProjectId.delete(queueName);
      }
    }
    
    console.log(`Bull Board: 成功从监控面板中移除队列 ${projectId}`);
  } catch (error) {
    // 队列移除失败不应该影响主流程，只记录错误
    console.error(`Bull Board: 移除队列 ${projectId} 时出错:`, error);
  }
} 