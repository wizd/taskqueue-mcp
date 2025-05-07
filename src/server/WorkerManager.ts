import { Worker, Job, WorkerOptions } from 'bullmq';
import { Redis } from 'ioredis';
import { BullMQTaskData, createRedisKeys, normalizeRedisPrefix } from '../types/bullmq.js';
import { RedisManager } from './RedisManager.js';
import { RedisOptions } from 'ioredis';
import { RedisNamingValidator } from './RedisNamingValidator.js';
import { Logger } from './Logger.js';
import { google } from '@ai-sdk/google';
import { generateText, GenerateTextResult } from 'ai';
import { Project } from '../types/data.js';

/**
 * Worker管理器 - 负责为所有项目队列创建和管理Worker实例
 */
export class WorkerManager {
  private workers: Map<string, Worker> = new Map();
  private redisManager: RedisManager;
  private redisOptions?: RedisOptions;
  private workerOptions?: Partial<WorkerOptions>;
  private prefix?: string;
  private logger: Logger;
  private initialized: boolean = false;
  private readProjectFunction?: (projectId: string) => Promise<Project>;

  /**
   * 创建WorkerManager实例
   * @param redisOptions Redis连接选项
   * @param workerOptions Worker配置选项
   * @param prefix 可选的全局前缀
   * @param readProjectFunction 可选的读取项目数据的函数
   */
  constructor(
    redisOptions?: RedisOptions,
    workerOptions?: WorkerOptions,
    prefix?: string,
    readProjectFunction?: (projectId: string) => Promise<Project>
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
    this.readProjectFunction = readProjectFunction; // 存储传入的函数
    this.logger.info('WorkerManager已创建，等待初始化');
  }

  /**
   * 初始化Worker管理器
   * 确保在使用任何需要Redis连接的方法前调用此方法
   */
  public async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    try {
      this.logger.info('初始化WorkerManager...');
      
      // 确保Redis管理器已初始化
      try {
        // 尝试获取连接 - 如果未初始化会抛出异常
        const existingConnection = this.redisManager.getConnection();
        this.logger.info('Redis连接已存在');
      } catch (error) {
        // 如果获取连接失败，初始化Redis连接
        this.logger.info('初始化Redis连接...');
        await this.redisManager.initialize();
      }
      
      // 获取Redis连接并设置Worker选项
      const connection = this.redisManager.getConnection();
      
      // 设置Worker选项
      this.workerOptions = {
        ...this.workerOptions,
        connection, // 确保使用初始化后的连接
        concurrency: 1, // 强制并发为1，实现项目内串行
        lockDuration: this.workerOptions?.lockDuration ?? 30000,
        stalledInterval: this.workerOptions?.stalledInterval ?? 30000,
        maxStalledCount: this.workerOptions?.maxStalledCount ?? 1,
        drainDelay: this.workerOptions?.drainDelay ?? 5,
        skipVersionCheck: this.workerOptions?.skipVersionCheck ?? false,
      };
      
      this.initialized = true;
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
    
    // 重新初始化所有Worker
    if (this.initialized && this.workers.size > 0) {
      this.logger.info(`由于前缀更改，重新初始化 ${this.workers.size} 个Worker`);
      this.shutdownAll().then(() => this.initAll());
    }
  }

  /**
   * 初始化所有项目的Worker
   * 扫描Redis中的所有项目元数据键，为每个项目创建Worker
   */
  public async initAll(): Promise<void> {
    // 确保已初始化
    await this.ensureInitialized();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 获取所有项目元数据键
      const projectPattern = redisKeys.projectMetadataPattern();
      this.logger.info(`扫描项目: ${projectPattern}`);
      const projectMetadataKeys = await redis.keys(projectPattern);
      
      if (!projectMetadataKeys || projectMetadataKeys.length === 0) {
        this.logger.info('未找到项目，跳过Worker初始化');
        return;
      }
      
      // 提取项目ID
      const regex = redisKeys.projectIdFromKeyRegex();
      const projectIds = projectMetadataKeys
        .map(key => {
          const match = key.match(regex);
          return match ? match[1] : null;
        })
        .filter(Boolean) as string[];
      
      this.logger.info(`发现 ${projectIds.length} 个项目，开始初始化Worker`);
      
      // 为每个项目注册Worker
      for (const projectId of projectIds) {
        await this.register(projectId);
      }
      
      this.logger.info(`Worker初始化完成，共 ${this.workers.size} 个Worker`);
    } catch (error) {
      this.logger.error('初始化Worker失败:', error);
      throw error;
    }
  }

  /**
   * 为特定项目注册Worker
   * @param projectId 项目ID
   */
  public async register(projectId: string): Promise<Worker> {
    // 确保已初始化
    await this.ensureInitialized();
    
    const redis = this.redisManager.getConnection();
    const redisKeys = this.getRedisKeys();
    const queueName = redisKeys.projectQueueName(projectId);
    
    // 检查Worker是否已存在
    if (this.workers.has(queueName)) {
      this.logger.info(`项目 ${projectId} 的Worker已存在，跳过创建`);
      return this.workers.get(queueName)!;
    }
    
    try {
      // 创建项目Worker
      this.logger.info(`为项目 ${projectId} 创建Worker (队列: ${queueName})`);
      
      if (!this.workerOptions || !this.workerOptions.connection) {
        throw new Error('Worker选项未正确初始化，无法创建Worker');
      }
      
      const worker = new Worker(
        queueName,
        this.processorFn.bind(this),
        {
          ...this.workerOptions,
          prefix: '', // 确保Worker与Queue使用相同的前缀设置
        } as WorkerOptions
      );
      
      // 监听Worker事件
      this.setupWorkerListeners(worker, projectId);
      
      // 存储Worker实例
      this.workers.set(queueName, worker);
      this.logger.info(`项目 ${projectId} 的Worker创建成功`);
      
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
    const queueName = redisKeys.projectQueueName(projectId);
    
    // 检查Worker是否存在
    if (!this.workers.has(queueName)) {
      this.logger.info(`项目 ${projectId} 的Worker不存在，跳过注销`);
      return;
    }
    
    try {
      // 获取Worker实例
      const worker = this.workers.get(queueName)!;
      
      // 关闭Worker
      this.logger.info(`关闭项目 ${projectId} 的Worker`);
      await worker.close();
      
      // 从映射中移除
      this.workers.delete(queueName);
      this.logger.info(`项目 ${projectId} 的Worker已注销`);
    } catch (error) {
      this.logger.error(`注销项目 ${projectId} 的Worker失败:`, error);
      throw error;
    }
  }

  /**
   * 关闭所有Worker
   */
  public async shutdownAll(): Promise<void> {
    if (!this.initialized || this.workers.size === 0) {
      this.logger.info('没有Worker需要关闭');
      return;
    }
    
    this.logger.info(`开始关闭 ${this.workers.size} 个Worker`);
    
    // 创建所有Worker关闭操作的Promise数组
    const closePromises = Array.from(this.workers.entries()).map(async ([queueName, worker]) => {
      try {
        this.logger.info(`关闭队列 ${queueName} 的Worker`);
        await worker.close();
        return queueName;
      } catch (error) {
        this.logger.error(`关闭队列 ${queueName} 的Worker失败:`, error);
        throw error;
      }
    });
    
    // 等待所有Worker关闭
    try {
      const closedQueues = await Promise.all(closePromises);
      
      // 清空workers映射
      this.workers.clear();
      
      this.logger.info(`成功关闭 ${closedQueues.length} 个Worker`);
    } catch (error) {
      this.logger.error('关闭Worker时发生错误:', error);
      throw error;
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
    
    try {
      this.logger.info(`开始处理任务 ${taskData.id} (${taskData.title}) (项目: ${projectId})`);
      await job.updateProgress(10);

      let projectContextString = "项目上下文不可用或获取失败。";
      if (this.readProjectFunction) {
        try {
          this.logger.info(`正在为任务 ${taskData.id} 获取项目 ${projectId} 的上下文...`);
          const projectContext: Project = await this.readProjectFunction(projectId);
          projectContextString = JSON.stringify(projectContext, null, 2);
          this.logger.info(`成功获取项目 ${projectId} 的上下文 (任务 ${taskData.id})`);
          await job.updateProgress(20);
        } catch (e) {
          this.logger.error(`获取项目 ${projectId} 上下文失败 (任务 ${taskData.id}):`, e);
          projectContextString = `获取项目上下文失败: ${e instanceof Error ? e.message : String(e)}`;
        }
      } else {
        this.logger.warn(`未提供 readProjectFunction 给 WorkerManager。无法获取任务 ${taskData.id} 的项目上下文。`);
      }

      const inProgressTaskData: BullMQTaskData = {
        ...taskData,
        status: "in progress",
        updatedAt: Date.now()
      };
      await job.updateData(inProgressTaskData);
      this.logger.info(`任务 ${taskData.id} 状态更新为 "in progress"`);
      
      const llmPrompt = 
`你好！这里有一些关于当前项目的背景信息，以及一个需要你协助处理的具体任务。请先仔细阅读这些材料。

<project_context>
${projectContextString}
</project_context>

接下来，这是你需要处理的具体任务：
<current_task>
ID: ${taskData.id}
Title: ${taskData.title}
Description: ${taskData.description}
Status: ${taskData.status}
Approved: ${taskData.approved}
${taskData.toolRecommendations ? `Tool Recommendations: ${taskData.toolRecommendations}` : ''}
${taskData.ruleRecommendations ? `Rule Recommendations: ${taskData.ruleRecommendations}` : ''}
</current_task>

现在，请你基于上述所有信息，像在平时对话那样，告诉我你将如何完成这项任务。请详细描述你的思考过程、计划采取的步骤、关键的观察点，以及预期的任务成果或结论。
你的回复将被直接用作该任务的"完成详情"（completedDetails）记录下来。
因此，请确保你的表述清晰、完整，并且紧扣任务的要求。谢谢！`;
      
      await job.updateProgress(30);

      let llmResultText = "LLM处理被跳过或遇到问题。使用默认完成详情。";
      try {
        this.logger.info(`开始为任务 ${taskData.id} 调用LLM...`);
        const modelProvider = google("gemini-2.0-flash-lite");
        const { text: generatedText } = await generateText({
            model: modelProvider,
            prompt: llmPrompt,
        });
        llmResultText = generatedText;
        this.logger.info(`LLM为任务 ${taskData.id} 推理成功。`);
        await job.updateProgress(80);
      } catch (llmError) {
        this.logger.error(`LLM为任务 ${taskData.id} 推理失败:`, llmError);
        llmResultText = `LLM推理失败: ${llmError instanceof Error ? llmError.message : String(llmError)}。原始任务描述: ${taskData.description}`;
      }
      
      const completedData: BullMQTaskData = {
        ...taskData,
        status: "done",
        completedDetails: llmResultText,
        updatedAt: Date.now()
      };
      
      await job.updateData(completedData);
      await job.updateProgress(100);
      
      this.logger.info(`任务 ${taskData.id} 处理完成，completedDetails已更新。`);
      
      return {
        taskId: taskData.id,
        status: "completed",
        completionTime: new Date().toISOString(),
        llmOutputSummary: llmResultText.substring(0, 200) + (llmResultText.length > 200 ? "..." : "")
      };
    } catch (error) {
      this.logger.error(`处理任务 ${taskData.id} 失败 (在 processorFn 的最外层捕获):`, error);
      
      const failedTaskData: BullMQTaskData = {
        ...taskData,
        status: "not started",
        completedDetails: `处理失败: ${error instanceof Error ? error.message : String(error)}`,
        updatedAt: Date.now()
      };
      
      try {
        await job.updateData(failedTaskData);
      } catch (updateError) {
        this.logger.error(`更新任务 ${taskData.id} 数据为失败状态时再次出错:`, updateError);
      }
      
      throw error;
    }
  }
} 