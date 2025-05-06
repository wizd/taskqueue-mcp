import { RedisOptions } from 'ioredis';
import { Job, Queue, Worker, QueueEvents, WorkerOptions, QueueOptions } from 'bullmq';

/**
 * BullMQ任务队列存储模式的配置选项
 */
export interface BullMQServiceOptions {
  /** Redis连接配置 */
  connection?: RedisOptions;
  /** 默认任务选项 */
  defaultJobOptions?: {
    attempts?: number;
    backoff?: {
      type: 'exponential' | 'fixed';
      delay: number;
    };
    removeOnComplete?: boolean | number;
    removeOnFail?: boolean | number;
  };
  /** 全局队列前缀，用于多应用环境 */
  prefix?: string;
  /** 默认项目队列选项 */
  projectQueueOptions?: QueueOptions;
  /** 默认工作进程选项 */
  workerOptions?: WorkerOptions;
}

/**
 * 表示BullMQ任务的数据结构
 */
export interface BullMQTaskData {
  id: string;
  title: string;
  description: string;
  status: "not started" | "in progress" | "done";
  approved: boolean;
  completedDetails: string;
  toolRecommendations?: string;
  ruleRecommendations?: string;
  projectId: string; // 关联的项目ID
}

/**
 * 表示BullMQ项目的数据结构
 */
export interface BullMQProjectData {
  projectId: string;
  initialPrompt: string;
  projectPlan: string;
  completed: boolean;
  autoApprove?: boolean;
  taskCount: number; // 任务总数计数
}

/**
 * Redis键命名工厂函数
 * 根据提供的前缀创建键生成函数
 * @param prefix 可选的前缀
 * @returns 一个包含键生成函数的对象
 */
export const createRedisKeys = (prefix?: string) => ({
  /** 项目元数据哈希表键 */
  projectMetadata: (projectId: string) => 
    `${prefix || ''}project:${projectId}:metadata`,
  /** 项目计数器 */
  projectCounter: () => 
    `${prefix || ''}taskqueue:counters:projects`,
  /** 任务计数器 */
  taskCounter: () => 
    `${prefix || ''}taskqueue:counters:tasks`,
  /** 项目任务清单集合 */
  projectTasks: (projectId: string) => 
    `${prefix || ''}project:${projectId}:tasks`,
  /** 获取项目队列名称 - 确保不包含冒号 */
  projectQueueName: (projectId: string) => {
    // BullMQ队列名称不能包含冒号
    // 使用统一的格式: tenant_tenantid_proj_projectid
    
    if (!prefix) {
      // 没有前缀的情况，直接返回
      return `proj_${projectId}`;
    }
    
    // 移除所有冒号，并确保格式一致
    const cleanPrefix = prefix.replace(/:/g, '_').replace(/^_+|_+$/g, '');
    
    // 如果是租户前缀格式(tenant:xxx:)，提取租户ID并使用统一格式
    const tenantMatch = prefix.match(/^tenant:([^:]+):/);
    if (tenantMatch && tenantMatch[1]) {
      return `tenant_${tenantMatch[1]}_proj_${projectId}`;
    }
    
    // 其他前缀格式
    return `${cleanPrefix}_proj_${projectId}`;
  },
  /** 用于 KEYS/SCAN 的项目元数据键模式 */
  projectMetadataPattern: () => 
    `${prefix || ''}project:proj-*:metadata`,
  /** 从 Redis 键中提取项目 ID 的正则表达式 */
  projectIdFromKeyRegex: () => 
    new RegExp(`^${prefix || ''}project:(proj-\\d+):metadata$`)
});

/**
 * 默认Redis键（无前缀）
 */
export const RedisKeys = createRedisKeys();

/**
 * BullMQ服务状态
 */
export enum BullMQServiceState {
  INITIALIZING = 'initializing',
  READY = 'ready',
  ERROR = 'error',
  CLOSED = 'closed'
}

/**
 * BullMQ迁移模式
 */
export enum MigrationMode {
  FILE_ONLY = 'file_only', // 仅使用文件存储
  BULLMQ_ONLY = 'bullmq_only', // 仅使用BullMQ存储
  DUAL_WRITE = 'dual_write', // 双写模式
  READ_BULLMQ_WRITE_BOTH = 'read_bullmq_write_both' // 读BullMQ写两者
}

/**
 * 存储类型指示器
 */
export type StorageType = 'file' | 'bullmq'; 