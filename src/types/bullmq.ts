import { RedisOptions } from 'ioredis';
import { Job, Queue, Worker, QueueEvents, WorkerOptions, QueueOptions } from 'bullmq';

/**
 * Defines the structure for the object returned by createRedisKeys.
 */
export interface IRedisKeys {
  projectMetadata: (projectId: string) => string;
  projectCounter: () => string;
  taskCounter: () => string;
  projectTasks: (projectId: string) => string;
  projectQueueName: (projectId: string) => string;
  projectMetadataPattern: () => string;
  projectIdFromKeyRegex: () => RegExp;
  newProjectRegistrationQueueName: () => string;
  getBullMQPrefix: () => string | undefined;
}

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
  tenantId?: string; // 新增：租户ID
  createdAt: number; // 新增：创建时间戳
  updatedAt: number; // 新增：更新时间戳
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
  tenantId?: string; // 新增：租户ID
  createdAt: number; // 新增：创建时间戳
  updatedAt: number; // 新增：更新时间戳
  projectConclusion?: string; // 新增：项目总结字段
}

/**
 * 规范化Redis前缀
 * 移除前导冒号和尾部冒号
 * @param prefix 原始前缀
 * @returns 规范化后的前缀
 */
export const normalizeRedisPrefix = (prefix?: string): string | undefined => {
  if (!prefix || prefix.trim() === '') {
    return undefined; // No prefix or empty prefix becomes undefined
  }
  // Ensure prefix ends with a colon if it's not already a system prefix like "bull" or "system"
  // And ensure it doesn't start with 'bull:' or 'system:' if it's not just 'bull' or 'system'
  if (prefix !== 'bull' && prefix !== 'system' && (prefix.startsWith('bull:') || prefix.startsWith('system:'))) {
    // This case is likely an error or misconfiguration, log or handle as error
    console.warn(`normalizeRedisPrefix: Prefix '${prefix}' starts with reserved keyword 'bull:' or 'system:'. This might lead to issues.`);
  }

  if (!prefix.endsWith(':') && !['bull', 'system'].includes(prefix)) {
    return `${prefix}:`;
  }
  return prefix;
};

/**
 * Redis键命名工厂函数
 * 根据提供的前缀创建键生成函数
 * @param prefixInput Optional prefix for all keys, typically for multi-tenancy.
 *                    If undefined or empty, some keys might default to a global scope or use BullMQ's default 'bull'.
 */
export const createRedisKeys = (prefixInput?: string): IRedisKeys => {
  // normalizedApiPrefix is the prefix string that should be used by BullMQ's `prefix` option.
  // It can be undefined if no prefixInput is provided.
  const normalizedApiPrefix = normalizeRedisPrefix(prefixInput);

  // effectiveBasePrefix is used for constructing non-queue Redis keys (like metadata, counters).
  // If normalizedApiPrefix is 'tenant:foo:', effectiveBasePrefix becomes 'tenant:foo:'.
  // If normalizedApiPrefix is undefined, effectiveBasePrefix becomes ''.
  const effectiveBasePrefix = normalizedApiPrefix || '';

  return {
    /** 项目元数据哈希表键 */
    projectMetadata: (projectId: string) => `${effectiveBasePrefix}project:${projectId}:metadata`,
    /** 项目计数器 */
    projectCounter: () => `${effectiveBasePrefix}system:counters:projects`,
    /** 任务计数器 */
    taskCounter: () => `${effectiveBasePrefix}system:counters:tasks`,
    /** 项目任务清单集合 */
    projectTasks: (projectId: string) => `${effectiveBasePrefix}project:${projectId}:tasks`,
    /** 获取项目队列名称 - 确保不包含冒号 */
    projectQueueName: (projectId: string) => `project-${projectId}`,
    /** 用于 KEYS/SCAN 的项目元数据键模式 */
    projectMetadataPattern: () => `${effectiveBasePrefix}project:*:metadata`,
    /** 从 Redis 键中提取项目 ID 的正则表达式 */
    projectIdFromKeyRegex: () => {
      // If there's a prefix, match after it. Otherwise, match from the start.
      const patternPrefix = effectiveBasePrefix.replace(/[.*+?^${}()|[\]\\]/g, '\\\$&'); // Escape regex special chars in prefix
      return new RegExp(`^${patternPrefix}project:(.+):metadata$`);
    },
    newProjectRegistrationQueueName: () => `system-new-project-registration`,
    /** 提供给BullMQ的完整前缀 */
    getBullMQPrefix: () => normalizedApiPrefix,
  };
};

/**
 * 默认Redis键（无前缀）
 */
export const RedisKeys: IRedisKeys = createRedisKeys();

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