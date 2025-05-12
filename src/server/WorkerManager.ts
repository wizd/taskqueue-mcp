import { Worker, Job, WorkerOptions } from 'bullmq';
import { BullMQTaskData, createRedisKeys, normalizeRedisPrefix, RedisKeys } from '../types/bullmq.js';
import { RedisManager } from './RedisManager.js';
import { RedisOptions } from 'ioredis';
import { Logger } from './Logger.js';
import { Project, Task } from '../types/data.js';
import { runTask } from '../../lib/worker/runTask.js';
import { checkAndConcludeProject } from '../../lib/worker/concludeProject.js';

/**
 * Represents the data structure for a project registration job.
 */
interface RegistrationJobData {
  projectId: string;
  tenantId?: string; // Optional tenant ID
}

/**
 * Worker管理器 - 负责为所有项目队列创建和管理Worker实例
 */
export class WorkerManager {
  private workers: Map<string, Worker> = new Map();
  private registrationWorker: Worker | null = null; // Worker for handling new project registrations
  private redisManager: RedisManager;
  private redisOptions?: RedisOptions;
  private workerOptions?: Partial<WorkerOptions>;
  private prefix?: string;
  private logger: Logger;
  private initialized: boolean = false;
  private readProjectFunction?: (projectId: string) => Promise<Project>;
  private finalizeProjectFunction?: (projectId: string, conclusion: string) => Promise<void>;

  /**
   * 创建WorkerManager实例
   * @param redisOptions Redis连接选项
   * @param workerOptions Worker配置选项
   * @param prefix 可选的全局前缀
   * @param readProjectFunction 可选的读取项目数据的函数
   * @param finalizeProjectFunction 可选的完成项目并保存总结的函数
   */
  constructor(
    redisOptions?: RedisOptions,
    workerOptions?: WorkerOptions,
    prefix?: string,
    readProjectFunction?: (projectId: string) => Promise<Project>,
    finalizeProjectFunction?: (projectId: string, conclusion: string) => Promise<void>
  ) {
    // 强制 maxRetriesPerRequest: null，确保 BullMQ 兼容
    this.redisOptions = {
      ...redisOptions,
      maxRetriesPerRequest: null,
    };
    this.workerOptions = workerOptions;
    this.prefix = normalizeRedisPrefix(prefix);
    
    // 获取RedisManager实例，但不立即使用连接
    this.redisManager = RedisManager.getInstance(this.redisOptions);
    
    // 初始化日志记录器
    this.logger = new Logger('WorkerManager');
    this.readProjectFunction = readProjectFunction;
    this.finalizeProjectFunction = finalizeProjectFunction;
  }

  /**
   * 初始化Worker管理器
   * 确保在使用任何需要Redis连接的方法前调用此方法
   */
  public async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.logger.info('初始化WorkerManager...');
    try {
      // Ensure RedisManager is initialized.
      // RedisManager.initialize() has its own guard, so direct call is fine.
      await this.redisManager.initialize();
      this.logger.info('RedisManager 已初始化或连接已存在。');

      const connection = this.redisManager.getConnection();
      
      this.workerOptions = {
        ...this.workerOptions,
        connection,
        concurrency: 1, // 强制并发为1，实现项目内串行
        lockDuration: this.workerOptions?.lockDuration ?? 30000,
        stalledInterval: this.workerOptions?.stalledInterval ?? 30000,
        maxStalledCount: this.workerOptions?.maxStalledCount ?? 1,
        drainDelay: this.workerOptions?.drainDelay ?? 5,
        skipVersionCheck: this.workerOptions?.skipVersionCheck ?? false,
      };

      // Critical: Set initialized to true BEFORE calling startRegistrationWorker,
      // which internally calls ensureInitialized.
      this.initialized = true;
      
      // 创建并启动注册 Worker
      await this.startRegistrationWorker();
      
      this.logger.info('WorkerManager初始化完成');
    } catch (error) {
      this.logger.error('WorkerManager初始化失败:', error);
      throw error;
    }
  }

  /**
   * 确保Worker管理器已初始化
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }

  /**
   * 获取带有当前前缀的RedisKeys工厂
   * @returns RedisKeys工厂函数
   */
  private getRedisKeys() {
    return createRedisKeys(this.prefix);
  }

  /**
   * 设置新的前缀
   * @param prefix 新前缀
   */
  public setPrefix(prefix?: string): void {
    if (this.prefix === prefix) return;
    
    const oldPrefix = this.prefix;
    this.prefix = normalizeRedisPrefix(prefix);
    this.logger.info(`前缀已更改: ${oldPrefix || '无'} -> ${this.prefix || '无'}`);
    
    // 重新初始化所有Worker (包括注册Worker)
    if (this.initialized) {
      this.logger.info(`由于前缀更改，重新初始化所有 Worker`);
      // 关闭所有现有Worker，然后重新启动注册Worker (项目Worker将按需创建)
      this.shutdownAll().then(async () => {
        this.logger.info('旧Worker已关闭，重新启动注册Worker');
        await this.startRegistrationWorker(); 
      });
    }
  }

  /**
   * 为特定项目注册Worker
   * @param projectId 项目ID
   */
  public async register(projectId: string): Promise<Worker> {
    // 确保已初始化
    await this.ensureInitialized();
    
    const redisKeys = this.getRedisKeys();
    const baseQueueName = redisKeys.projectQueueName(projectId); // Get base name
    const bullmqOptPrefix = redisKeys.getBullMQPrefix() ?? ''; // Get prefix, default to ''

    const mapKey = bullmqOptPrefix ? `${bullmqOptPrefix}:${baseQueueName}` : baseQueueName;
    
    // 检查Worker是否已存在
    if (this.workers.has(mapKey)) {
      this.logger.info(`项目 ${projectId} 的Worker (队列 ${mapKey}) 已存在，跳过创建`);
      return this.workers.get(mapKey)!;
    }
    
    try {
      // 创建项目Worker
      this.logger.info(`为项目 ${projectId} 创建Worker (队列: ${mapKey})`);
      
      if (!this.workerOptions || !this.workerOptions.connection) {
        throw new Error('Worker选项未正确初始化，无法创建Worker');
      }
      
      const worker = new Worker<BullMQTaskData>(
        baseQueueName, // Use base name
        this.processorFn.bind(this),
        {
          ...this.workerOptions,
          prefix: bullmqOptPrefix, // Use explicit prefix
        } as WorkerOptions
      );
      
      // 监听Worker事件
      this.setupWorkerListeners(worker, projectId);
      
      // 存储Worker实例
      this.workers.set(mapKey, worker);
      this.logger.info(`项目 ${projectId} 的Worker创建成功 (队列 ${mapKey})`);
      
      return worker;
    } catch (error) {
      this.logger.error(`为项目 ${projectId} 创建Worker失败:`, error);
      throw error;
    }
  }

  /**
   * 设置Worker事件监听器
   * @param worker Worker实例
   * @param projectId 关联的项目ID
   */
  private setupWorkerListeners(worker: Worker, projectId: string): void {
    // 任务完成事件
    worker.on('completed', (job: Job, result: any) => {
      this.logger.info(`项目 ${projectId} 的任务 ${job.id} 已完成: ${JSON.stringify(result)}`);
    });
    
    // 任务失败事件
    worker.on('failed', (job: Job | undefined, error: Error) => {
      if (job) {
        this.logger.error(`项目 ${projectId} 的任务 ${job.id} 失败:`, error);
      } else {
        this.logger.error(`项目 ${projectId} 的未知任务失败:`, error);
      }
    });
    
    // 一般错误事件
    worker.on('error', (error: Error) => {
      this.logger.error(`项目 ${projectId} 的Worker发生错误:`, error);
    });
    
    // 任务活动事件
    worker.on('active', (job: Job) => {
      this.logger.info(`项目 ${projectId} 的任务 ${job.id} 开始处理`);
    });
    
    // 任务停滞事件
    worker.on('stalled', (jobId: string) => {
      this.logger.warn(`项目 ${projectId} 的任务 ${jobId} 已停滞`);
    });
  }

  /**
   * 注销项目的Worker
   * @param projectId 项目ID
   */
  public async unregister(projectId: string): Promise<void> {
    // 确保已初始化
    await this.ensureInitialized();
    
    const redisKeys = this.getRedisKeys();
    const baseQueueName = redisKeys.projectQueueName(projectId); // Get base name
    const bullmqOptPrefix = redisKeys.getBullMQPrefix() ?? ''; // Get prefix, default to ''
    const mapKey = bullmqOptPrefix ? `${bullmqOptPrefix}:${baseQueueName}` : baseQueueName;
        
    // 检查Worker是否存在
    if (!this.workers.has(mapKey)) {
      this.logger.info(`项目 ${projectId} 的Worker (队列 ${mapKey}) 不存在，跳过注销`);
      return;
    }
    
    try {
      // 获取Worker实例
      const worker = this.workers.get(mapKey)!;
      
      // 关闭Worker
      this.logger.info(`关闭项目 ${projectId} 的Worker (队列 ${mapKey})`);
      await worker.close();
      
      // 从映射中移除
      this.workers.delete(mapKey);
      this.logger.info(`项目 ${projectId} 的Worker已注销 (队列 ${mapKey})`);
    } catch (error) {
      this.logger.error(`注销项目 ${projectId} 的Worker失败:`, error);
      throw error;
    }
  }

  /**
   * 关闭所有Worker
   */
  public async shutdownAll(): Promise<void> {
    if (!this.initialized) {
        // If not initialized, cannot have workers.
        this.logger.info('WorkerManager未初始化，没有Worker需要关闭');
        return;
    }

    const projectWorkerCount = this.workers.size;
    const registrationWorkerExists = !!this.registrationWorker;

    if (projectWorkerCount === 0 && !registrationWorkerExists) {
        this.logger.info('没有活动的Worker需要关闭');
        return;
    }

    this.logger.info(`开始关闭 ${projectWorkerCount} 个项目Worker${registrationWorkerExists ? ' 和 注册Worker' : ''}`);

    const closePromises: Promise<string | null>[] = []; // Explicitly type the promise array

    // Close project workers
    this.workers.forEach((worker, mapKey) => { // mapKey is the fully qualified name
        closePromises.push((async () => {
            try {
                this.logger.info(`关闭项目队列 ${mapKey} 的Worker`);
                await worker.close();
                return mapKey; // Success
            } catch (error) {
                this.logger.error(`关闭项目队列 ${mapKey} 的Worker失败:`, error);
                return null; // Failure
            }
        })());
    });
    // Clear the project worker map immediately after initiating close
    this.workers.clear();


    // Close registration worker
    if (this.registrationWorker) {
        // Assign to a temp variable to avoid race conditions with the async operation
        const workerToClose = this.registrationWorker;
        const workerNameForLog = workerToClose.name; // Base name
        const workerPrefixForLog = (workerToClose as any).opts?.prefix ?? ''; // Actual prefix used
        const fullWorkerNameForLog = workerPrefixForLog ? `${workerPrefixForLog}:${workerNameForLog}` : workerNameForLog;

        // Set to null immediately BEFORE awaiting close, to prevent race condition in restart logic.
        this.registrationWorker = null;
        this.logger.info('Registration worker instance reference cleared.');

        closePromises.push((async () => {
            try {
                this.logger.info(`关闭注册Worker (队列: ${fullWorkerNameForLog})`);
                await workerToClose.close();
                this.logger.info('注册Worker已成功关闭');
                return fullWorkerNameForLog; // Success
            } catch (error) {
                this.logger.error(`关闭注册Worker (队列: ${fullWorkerNameForLog}) 失败:`, error);
                // this.registrationWorker is already null
                return null; // Failure
            }
        })());
    }
    
    // 等待所有Worker关闭
    try {
      const results = await Promise.all(closePromises);
      const closedItems = results.filter(r => r !== null);
      this.logger.info(`关闭操作完成，成功关闭 ${closedItems.length} 个Worker`);
    } catch (error) {
      // This catch is less likely now as individual errors are caught
      this.logger.error('等待Worker关闭时发生意外错误:', error);
    }
  }

  /**
   * 检查WorkerManager是否已初始化
   */
  public isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * 任务处理函数 - 处理队列中的任务
   * @param job 当前处理的任务
   */
  private async processorFn(job: Job<BullMQTaskData>): Promise<any> {
    const taskData = job.data;
    const projectId = taskData.projectId;
    
    const taskResult = await runTask(taskData, projectId, job, this.logger, this.readProjectFunction, this.finalizeProjectFunction);

    // 检查项目是否所有任务都已完成，并执行项目总结（如果需要）
    await checkAndConcludeProject(projectId, this.logger, this.readProjectFunction, this.finalizeProjectFunction);

    return taskResult;
  }

  /**
   * Starts the worker responsible for listening to the new project registration queue.
   */
  private async startRegistrationWorker(): Promise<void> {
    await this.ensureInitialized(); // Ensure Redis connection is ready

    if (this.registrationWorker) {
      this.logger.warn('Registration worker already running. Skipping start.');
      return;
    }

    const redisKeys = this.getRedisKeys();
    const baseRegistrationQueueName = redisKeys.newProjectRegistrationQueueName(); // Get base name
    const bullmqOptPrefix = redisKeys.getBullMQPrefix() ?? ''; // Get prefix, default to ''
    const connection = this.redisManager.getConnection();

    const fullQueueNameForLog = bullmqOptPrefix ? `${bullmqOptPrefix}:${baseRegistrationQueueName}` : baseRegistrationQueueName;

    this.logger.info(`Starting registration worker for queue: ${fullQueueNameForLog}`);

    try {
      this.registrationWorker = new Worker<RegistrationJobData>(
        baseRegistrationQueueName, // Use base name
        this.handleRegistrationJob.bind(this),
        {
          connection: connection,
          concurrency: 5, 
          prefix: bullmqOptPrefix, // Use explicit prefix
        }
      );

      this.registrationWorker.on('completed', (job: Job<RegistrationJobData>) => {
        this.logger.info(`Registration job for project ${job.data.projectId} completed (queue: ${fullQueueNameForLog}).`);
      });

      this.registrationWorker.on('failed', (job: Job<RegistrationJobData> | undefined, error: Error) => {
        const projectId = job?.data?.projectId ?? 'unknown';
        this.logger.error(`Registration job for project ${projectId} failed (queue: ${fullQueueNameForLog}):`, error);
      });

      this.registrationWorker.on('error', (error: Error) => {
        this.logger.error(`Registration worker (queue: ${fullQueueNameForLog}) encountered an error:`, error);
      });

      this.logger.info(`Registration worker started successfully for queue ${fullQueueNameForLog}.`);

    } catch (error) {
      this.logger.error(`Failed to start registration worker for queue ${fullQueueNameForLog}:`, error);
      this.registrationWorker = null; // Ensure it's null if startup failed
      throw error; // Re-throw the error to signal initialization failure
    }
  }

  /**
   * Handles jobs from the new project registration queue.
   * @param job The registration job containing the projectId.
   */
  private async handleRegistrationJob(job: Job<RegistrationJobData>): Promise<void> {
    const { projectId, tenantId } = job.data;
    this.logger.info(`Received registration request for project ${projectId} (Tenant: ${tenantId || 'N/A'})`);

    try {
      // Attempt to register the worker for the new project
      await this.register(projectId);
      this.logger.info(`Successfully registered worker for project ${projectId}.`);
    } catch (error) {
      this.logger.error(`Failed to register worker for project ${projectId} during registration job:`, error);
      // Optional: Implement retry logic or move to a failed queue if needed
      throw error; // Re-throw to mark the job as failed in BullMQ
    }
  }
} 