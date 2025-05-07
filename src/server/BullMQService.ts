import { Queue, Worker, QueueEvents, Job, FlowProducer } from 'bullmq';
import { Redis } from 'ioredis';
import { AppError, AppErrorCode } from '../types/errors.js';
import { RedisManager } from './RedisManager.js';
import { BullMQServiceOptions, BullMQServiceState, RedisKeys, BullMQTaskData, BullMQProjectData, createRedisKeys, normalizeRedisPrefix } from '../types/bullmq.js';
import { Task, Project } from '../types/data.js';
import { addQueueToBoard, removeQueueFromBoard } from './bullBoardMonitor.js';
import { RedisNamingValidator } from './RedisNamingValidator.js';
import { WorkerManager } from './WorkerManager.js';

/**
 * BullMQ服务类
 * 负责管理BullMQ队列和作业操作
 */
export class BullMQService {
  private redisManager: RedisManager;
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private queueEvents: Map<string, QueueEvents> = new Map();
  private flowProducer: FlowProducer | null = null;
  private serviceState: BullMQServiceState = BullMQServiceState.INITIALIZING;
  private options: BullMQServiceOptions;
  private projectCounter: number = 0;
  private taskCounter: number = 0;
  private workerManager: WorkerManager;

  /**
   * 创建BullMQService实例
   * @param options BullMQ服务配置选项
   */
  constructor(options: BullMQServiceOptions = {}) {
    this.options = options;
    this.redisManager = RedisManager.getInstance(options.connection);
    
    // 创建WorkerManager实例，并传入 readProject 和 finalizeProject 方法
    this.workerManager = new WorkerManager(
      options.connection,
      options.workerOptions,
      options.prefix,
      this.readProject.bind(this),
      this.finalizeProject.bind(this) // 新增参数
    );
    
    this.initialize();
  }

  /**
   * 初始化BullMQ服务
   */
  private async initialize(): Promise<void> {
    try {
      // 初始化Redis连接
      await this.redisManager.initialize();
      
      // 初始化Flow Producer
      this.flowProducer = new FlowProducer({
        connection: this.redisManager.getConnection(),
        prefix: ''
      });

      // 加载计数器
      await this.loadCounters();
      
      // 初始化WorkerManager - 为现有项目创建Worker
      await this.workerManager.initialize(); // 先初始化WorkerManager
      await this.workerManager.initAll();    // 然后初始化所有Worker
      
      this.serviceState = BullMQServiceState.READY;
    } catch (error) {
      this.serviceState = BullMQServiceState.ERROR;
      console.error('BullMQ服务初始化失败:', error);
      throw new AppError(
        'BullMQ服务初始化失败',
        AppErrorCode.ServiceNotReadyError,
        error
      );
    }
  }

  /**
   * 加载项目和任务计数器
   */
  private async loadCounters(): Promise<void> {
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 获取项目计数器，如果不存在则初始化为0
      const projectCounterKey = redisKeys.projectCounter();
      const projectCounter = await redis.get(projectCounterKey);
      this.projectCounter = projectCounter ? parseInt(projectCounter, 10) : 0;
      
      // 获取任务计数器，如果不存在则初始化为0
      const taskCounterKey = redisKeys.taskCounter();
      const taskCounter = await redis.get(taskCounterKey);
      this.taskCounter = taskCounter ? parseInt(taskCounter, 10) : 0;
    } catch (error) {
      throw new AppError(
        '加载计数器失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 检查服务是否就绪
   */
  private ensureReady(): void {
    if (this.serviceState !== BullMQServiceState.READY) {
      throw new AppError(
        'BullMQ服务未就绪',
        AppErrorCode.ServiceNotReadyError
      );
    }
  }

  /**
   * 使用重试机制获取 BullMQ Job
   * @param queue 目标队列
   * @param jobId 任务 ID
   * @param retries 重试次数
   * @param delay 重试间隔 (ms)
   * @returns Job 对象或 null
   */
  private async _getJobWithRetry(queue: Queue, jobId: string, retries = 5, delay = 500): Promise<Job | null> {
    for (let i = 0; i < retries; i++) {
      try {
        const job = await queue.getJob(jobId);
        if (job) {
          return job;
        }
      } catch (error) {
        // BullMQ 的 getJob 可能会因为 job 不存在或其他内部错误而抛出异常
        console.warn(`Error fetching job ${jobId} from queue ${queue.name} (attempt ${i + 1}/${retries}):`, error);
      }
      if (i < retries - 1) {
        // console.log(`Job ${jobId} not found in queue ${queue.name}, attempt ${i + 1}/${retries}. Retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    // console.warn(`Job ${jobId} not found in queue ${queue.name} after ${retries} attempts.`);
    return null; // 明确返回 null，让调用方处理
  }

  /**
   * 获取当前设置的前缀
   * @returns 当前前缀
   */
  public getCurrentPrefix(): string | undefined {
    return this.options.prefix;
  }

  /**
   * 获取带有当前前缀的RedisKeys
   * @returns 带有当前前缀的RedisKeys
   */
  private getRedisKeys() {
    // 使用normalizeRedisPrefix确保前缀格式正确
    const normalizedPrefix = normalizeRedisPrefix(this.getCurrentPrefix());
    return createRedisKeys(normalizedPrefix);
  }

  /**
   * 获取项目队列
   * @param projectId 项目ID
   * @returns 项目队列实例
   */
  public getProjectQueue(projectId: string): Queue {
    this.ensureReady();
    
    const redisKeys = this.getRedisKeys();
    const normalizedQueueName = redisKeys.projectQueueName(projectId);
    
    if (!this.queues.has(normalizedQueueName)) {
      try {
        console.log(`创建队列: ${normalizedQueueName} (项目ID: ${projectId})`);
        
        // 使用标准化的队列名创建队列
        const queue = new Queue(normalizedQueueName, {
          connection: this.redisManager.getConnection(),
          // 完全禁用任何前缀，因为我们的队列名已经包含了所有必要的命名空间信息
          prefix: '',
          ...this.options.projectQueueOptions
        });
        this.queues.set(normalizedQueueName, queue);
        
        // 当创建新队列时，自动添加到 Bull Board
        try {
          // 使用异步调用但不等待结果，以避免阻塞
          // 传递当前前缀以确保Bull Board使用相同的队列名称
          addQueueToBoard(projectId, this.getCurrentPrefix()).catch(error => {
            // 只记录错误但不影响队列创建
            console.warn(`将队列 ${normalizedQueueName} 添加到 Bull Board 失败:`, error);
          });
        } catch (error) {
          // 忽略错误，即使 Bull Board 添加失败也不影响队列正常工作
          console.warn(`尝试将队列 ${normalizedQueueName} 添加到 Bull Board 时出错:`, error);
        }
      } catch (error) {
        console.error(`创建队列 ${normalizedQueueName} 时出错:`, error);
        throw error;
      }
    }
    
    return this.queues.get(normalizedQueueName)!;
  }

  /**
   * 从前缀中提取租户ID
   * @returns 租户ID或undefined
   */
  private _getTenantIdFromPrefix(): string | undefined {
    if (this.options.prefix && this.options.prefix.startsWith('tenant:') && this.options.prefix.endsWith(':')) {
      // 提取 tenant:XXX: 中的 XXX 部分
      return this.options.prefix.substring('tenant:'.length, this.options.prefix.length - 1);
    }
    return undefined;
  }

  /**
   * 创建新项目
   * @param initialPrompt 初始提示
   * @param projectPlan 项目计划
   * @param autoApprove 是否自动审批
   * @returns 创建的项目ID
   */
  public async createProject(
    initialPrompt: string,
    projectPlan?: string,
    autoApprove = true
  ): Promise<string> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 增加项目计数器 - 使用带前缀的键
      const projectCounterKey = redisKeys.projectCounter();
      this.projectCounter = await redis.incr(projectCounterKey);
      const projectId = `proj-${this.projectCounter}`;
      
      // 获取租户ID
      const tenantId = this._getTenantIdFromPrefix();

      // 创建当前时间戳
      const currentTime = Date.now();

      // 创建项目元数据
      const projectData: BullMQProjectData = {
        projectId,
        initialPrompt,
        projectPlan: projectPlan || initialPrompt,
        completed: false,
        autoApprove,
        taskCount: 0,
        // 如果tenantId存在，则添加它
        ...(tenantId !== undefined && { tenantId }),
        createdAt: currentTime,
        updatedAt: currentTime,
      };
      
      // 存储项目元数据到Redis哈希表 - 使用带前缀的键
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      await redis.hset(
        projectMetadataKey,
        this.flattenObject(projectData)
      );
      
      // 确保创建项目队列
      this.getProjectQueue(projectId);
      
      // 为新项目注册Worker，确保WorkerManager已初始化
      try {
        if (!this.workerManager.isInitialized()) {
          await this.workerManager.initialize();
        }
        await this.workerManager.register(projectId);
      } catch (error) {
        console.warn(`为项目 ${projectId} 注册Worker失败:`, error);
        // 继续创建项目，但记录警告
      }
      
      return projectId;
    } catch (error) {
      throw new AppError(
        '创建项目失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 添加任务到项目
   * @param projectId 项目ID
   * @param tasks 任务列表
   * @returns 已添加的任务ID列表
   */
  public async addTasksToProject(
    projectId: string,
    tasks: { title: string; description: string; toolRecommendations?: string; ruleRecommendations?: string }[]
  ): Promise<string[]> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 检查项目是否存在
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      const exists = await redis.exists(projectMetadataKey);
      if (exists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      // 获取项目元数据（包含tenantId，如果存在）
      const projectData = await this.getProjectData(projectId);
      if (projectData.completed) {
        throw new AppError(
          '项目已完成，无法添加任务',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      const queue = this.getProjectQueue(projectId);
      const taskIds: string[] = [];
      
      // 从项目数据中获取租户ID，或者从当前服务前缀中获取
      // 此处优先使用项目已有的tenantId (如果有)，保证任务的tenantId与项目一致
      // 如果项目没有tenantId (例如旧数据), 则尝试从当前服务前缀获取
      const tenantId = projectData.tenantId || this._getTenantIdFromPrefix();
      
      // 创建当前时间戳
      const currentTime = Date.now();

      // 添加每个任务到队列
      for (const taskDef of tasks) {
        const taskCounterKey = redisKeys.taskCounter();
        this.taskCounter = await redis.incr(taskCounterKey);
        const taskId = `task-${this.taskCounter}`;
        
        const taskData: BullMQTaskData = {
          id: taskId,
          title: taskDef.title,
          description: taskDef.description,
          status: "not started",
          approved: false,
          completedDetails: "",
          toolRecommendations: taskDef.toolRecommendations,
          ruleRecommendations: taskDef.ruleRecommendations,
          projectId,
          // 如果tenantId存在，则添加它
          ...(tenantId !== undefined && { tenantId }),
          createdAt: currentTime,
          updatedAt: currentTime,
        };
        
        // 添加任务到项目队列，将 taskId 作为 jobId
        await queue.add('task', taskData, { 
          jobId: taskId, // 使用 taskId 作为 jobId
          ...this.options.defaultJobOptions 
        });
        
        // 添加任务ID到项目任务集合
        const projectTasksKey = redisKeys.projectTasks(projectId);
        await redis.sadd(projectTasksKey, taskId);
        
        taskIds.push(taskId);
      }
      
      // 更新项目任务计数
      await redis.hincrbyfloat(
        redisKeys.projectMetadata(projectId),
        'taskCount',
        tasks.length
      );
      
      // 更新项目的更新时间
      await redis.hset(
        redisKeys.projectMetadata(projectId),
        'updatedAt',
        currentTime.toString()
      );
      
      return taskIds;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '添加任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 获取项目数据
   * @param projectId 项目ID
   * @returns 项目数据
   */
  public async getProjectData(projectId: string): Promise<BullMQProjectData> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      const projectDataFromRedis = await redis.hgetall(projectMetadataKey);
      if (!projectDataFromRedis || Object.keys(projectDataFromRedis).length === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      const rawTenantId = projectDataFromRedis.tenantId;
      const tenantId = rawTenantId === 'undefined' ? undefined : (rawTenantId || undefined);
      
      const createdAt = projectDataFromRedis.createdAt ? parseInt(projectDataFromRedis.createdAt, 10) : Date.now();
      const updatedAt = projectDataFromRedis.updatedAt ? parseInt(projectDataFromRedis.updatedAt, 10) : Date.now();

      return {
        projectId,
        initialPrompt: projectDataFromRedis.initialPrompt,
        projectPlan: projectDataFromRedis.projectPlan,
        completed: projectDataFromRedis.completed === 'true',
        autoApprove: projectDataFromRedis.autoApprove === 'true',
        taskCount: parseInt(projectDataFromRedis.taskCount || '0', 10),
        ...(tenantId !== undefined && { tenantId }),
        createdAt,
        updatedAt,
        projectConclusion: projectDataFromRedis.projectConclusion,
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '获取项目数据失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 获取任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @returns 任务数据
   */
  public async getTask(projectId: string, taskId: string): Promise<BullMQTaskData> {
    this.ensureReady();
    
    try {
      const queue = this.getProjectQueue(projectId);
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (!job) {
        throw new AppError(
          `任务 ${taskId} 不存在`,
          AppErrorCode.TaskNotFound
        );
      }
      
      const taskData = job.data as BullMQTaskData;
      
      // 确保时间戳字段存在
      if (!taskData.createdAt) {
        taskData.createdAt = Date.now();
      }
      if (!taskData.updatedAt) {
        taskData.updatedAt = Date.now();
      }
      
      return taskData;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '获取任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 更新任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @param updates 任务更新数据
   * @returns 更新后的任务数据
   */
  public async updateTask(
    projectId: string,
    taskId: string,
    updates: {
      title?: string;
      description?: string;
      status?: "not started" | "in progress" | "done";
      completedDetails?: string;
      toolRecommendations?: string;
      ruleRecommendations?: string;
    }
  ): Promise<BullMQTaskData> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 检查任务是否存在于项目中
      const projectTasksKey = redisKeys.projectTasks(projectId);
      const isMember = await redis.sismember(projectTasksKey, taskId);
      if (!isMember) {
        console.error(`任务 ${taskId} 不存在于项目 ${projectId} 的任务集合 ${projectTasksKey} 中。`);
        try {
            const members = await redis.smembers(projectTasksKey);
            console.error(`当前集合 ${projectTasksKey} 内容: ${members.join(', ')}`);
        } catch (smembersError) {
            console.error(`获取集合 ${projectTasksKey} 内容时出错:`, smembersError);
        }
        throw new AppError(
          `任务 ${taskId} 不存在于项目 ${projectId} 中`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取队列
      const queue = this.getProjectQueue(projectId);
      
      // 尝试获取 Job 对象
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (!job) {
        throw new AppError(
          `无法获取任务 ${taskId} 的 Job 对象进行更新`,
          AppErrorCode.TaskNotFound
        );
      }
      
      const taskData = job.data as BullMQTaskData;
      
      // 检查任务是否已审批
      if (taskData.approved) {
        throw new AppError(
          '无法修改已审批的任务',
          AppErrorCode.CannotModifyApprovedTask
        );
      }
      
      // 创建当前时间戳
      const currentTime = Date.now();
      
      // 应用更新
      const updatedData: BullMQTaskData = {
        ...taskData,
        ...updates,
        updatedAt: currentTime
      };
      
      // 通过 BullMQ API 更新任务数据
      await job.updateData(updatedData);
      
      // 如果任务状态变为"完成"，也更新项目的更新时间
      if (updates.status === "done" && taskData.status !== "done") {
        await redis.hset(
          redisKeys.projectMetadata(projectId),
          'updatedAt',
          currentTime.toString()
        );
      }
      
      return updatedData;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '更新任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 审批任务完成
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @returns 更新后的任务数据
   */
  public async approveTaskCompletion(projectId: string, taskId: string): Promise<BullMQTaskData> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 首先检查任务是否存在于项目中
      const projectTasksKey = redisKeys.projectTasks(projectId);
      const isMember = await redis.sismember(projectTasksKey, taskId);
      if (!isMember) {
        throw new AppError(
          `任务 ${taskId} 不存在于项目 ${projectId} 中`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取队列
      const queue = this.getProjectQueue(projectId);
      
      // 先尝试通过可靠方法获取任务数据
      const taskData = await this._getTaskDataWithFallback(queue, taskId);
      
      if (!taskData) {
        throw new AppError(
          `无法获取任务 ${taskId} 的数据`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 检查任务状态
      if (taskData.status !== "done") {
        throw new AppError(
          '任务尚未完成',
          AppErrorCode.TaskNotDone
        );
      }
      
      // 检查任务是否已审批
      if (taskData.approved) {
        throw new AppError(
          '任务已审批',
          AppErrorCode.TaskAlreadyApproved
        );
      }
      
      // 创建当前时间戳
      const currentTime = Date.now();
      
      // 更新审批状态
      const updatedData: BullMQTaskData = {
        ...taskData,
        approved: true,
        updatedAt: currentTime
      };
      
      // 尝试获取Job对象更新任务数据
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (job) {
        // 通过BullMQ API更新任务数据
        await job.updateData(updatedData);
      } else {
        // 如果无法获取Job对象，尝试直接更新Redis数据
        console.warn(`approveTaskCompletion: BullMQ Job object for task ${taskId} not found, attempting to update Redis data directly.`);
        
        try {
          // 尝试直接找到任务数据的Redis键
          const queueIdKey = `${queue.name}:id`;
          const jobIds = await redis.hgetall(queueIdKey);
          
          // 查找与taskId匹配的内部ID
          let internalJobId: string | null = null;
          for (const [id, value] of Object.entries(jobIds)) {
            if (value === taskId) {
              internalJobId = id;
              break;
            }
          }
          
          if (!internalJobId) {
            throw new AppError(
              `找不到任务 ${taskId} 的内部ID映射`,
              AppErrorCode.TaskNotFound
            );
          }
          
          // 构造任务数据的Redis键
          const jobDataKey = `${queue.name}:${internalJobId}`;
          const jobData = await redis.hgetall(jobDataKey);
          
          if (!jobData || Object.keys(jobData).length === 0) {
            throw new AppError(
              `任务 ${taskId} 的Redis数据记录不存在`,
              AppErrorCode.TaskNotFound
            );
          }
          
          // 更新任务数据
          await redis.hset(jobDataKey, 'data', JSON.stringify(updatedData));
          
          console.log(`approveTaskCompletion: Successfully updated Redis data directly for task ${taskId}`);
        } catch (error) {
          console.error(`approveTaskCompletion: Failed to update Redis data directly for task ${taskId}:`, error);
          throw new AppError(
            `无法审批任务 ${taskId}`,
            AppErrorCode.JobProcessingError,
            error
          );
        }
      }
      
      // 更新项目的更新时间
      await redis.hset(
        redisKeys.projectMetadata(projectId),
        'updatedAt',
        currentTime.toString()
      );
      
      return updatedData;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '审批任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 审批项目完成
   * @param projectId 项目ID
   * @returns 成功消息
   */
  public async approveProjectCompletion(projectId: string): Promise<string> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 获取项目数据
      const projectData = await this.getProjectData(projectId);
      
      if (projectData.completed) {
        throw new AppError(
          '项目已完成',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      // 获取所有任务 ID
      const taskIds = await redis.smembers(redisKeys.projectTasks(projectId));
      const queue = this.getProjectQueue(projectId);
      
      // 获取所有任务的 Job 对象
      const jobPromises = taskIds.map(id => this._getJobWithRetry(queue, id));
      const jobs = await Promise.all(jobPromises);
      
      // 提取任务数据，过滤掉 null
      const tasksData = jobs.filter(job => job !== null).map(job => job!.data as BullMQTaskData);
      
      // 检查是否所有任务都获取成功
      if (tasksData.length < taskIds.length) {
          console.warn(`approveProjectCompletion: Retrieved ${tasksData.length} tasks out of ${taskIds.length} task IDs for project ${projectId}. Cannot approve completion without full data.`);
           throw new AppError(
             `无法获取项目 ${projectId} 的所有任务状态以进行审批`,
             AppErrorCode.TaskNotFound
           );
      }
      
      // 如果项目没有任务，则可以直接审批
      if (taskIds.length === 0) {
          console.log(`approveProjectCompletion: Project ${projectId} has no tasks, approving completion.`);
      } else {
          // 检查所有任务是否都已完成和审批
          const allDone = tasksData.every(task => task.status === "done");
          if (!allDone) {
            throw new AppError(
              '不是所有任务都已完成',
              AppErrorCode.TasksNotAllDone
            );
          }
          
          const allApproved = tasksData.every(task => task.approved);
          if (!allApproved) {
            throw new AppError(
              '不是所有已完成的任务都已审批',
              AppErrorCode.TasksNotAllApproved
            );
          }
      }
      
      // 创建当前时间戳
      const currentTime = Date.now();
      
      // 更新项目为已完成
      await redis.hset(
        redisKeys.projectMetadata(projectId),
        {
          'completed': 'true',
          'updatedAt': currentTime.toString()
        }
      );
      
      return `项目 ${projectId} 已完成并审批`;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '项目完成审批失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 完成项目，保存总结并标记为已完成
   * @param projectId 项目ID
   * @param conclusion LLM生成的项目总结
   */
  public async finalizeProject(projectId: string, conclusion: string): Promise<void> {
    this.ensureReady();

    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();

      // 检查项目是否存在
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      const exists = await redis.exists(projectMetadataKey);
      if (exists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在，无法完成。`,
          AppErrorCode.ProjectNotFound
        );
      }

      const currentTime = Date.now();

      // 更新项目元数据
      await redis.hset(projectMetadataKey, {
        projectConclusion: conclusion,
        completed: 'true',
        updatedAt: currentTime.toString(),
      });

      console.log(`项目 ${projectId} 已完成，总结已保存。`);
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        `完成项目 ${projectId} 失败`,
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 获取下一个要处理的任务
   * @param projectId 项目ID
   * @returns 下一个未完成或未审批的任务
   */
  public async getNextTask(projectId: string): Promise<BullMQTaskData | null> {
    this.ensureReady();
    
    try {
      // 获取项目数据
      const projectData = await this.getProjectData(projectId);
      
      if (projectData.completed) {
        throw new AppError(
          '项目已完成',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      const redis = this.redisManager.getConnection();
      const queue = this.getProjectQueue(projectId);
      const redisKeys = this.getRedisKeys();
      
      // 获取所有任务ID
      const taskIds = await redis.smembers(redisKeys.projectTasks(projectId));
      
      if (taskIds.length === 0) {
        throw new AppError(
          '项目没有任务',
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取所有任务的 Job 对象
      const jobPromises = taskIds.map(id => this._getJobWithRetry(queue, id));
      const jobs = await Promise.all(jobPromises);
      
      // 提取任务数据，过滤掉 null (获取失败的 Job)
      const tasksData = jobs.filter(job => job !== null).map(job => job!.data as BullMQTaskData);
      
      if (tasksData.length < taskIds.length) {
          console.warn(`getNextTask: Retrieved ${tasksData.length} tasks out of ${taskIds.length} task IDs for project ${projectId}. Some jobs might not be available yet or failed to retrieve.`);
      }
      
      // 如果完全没有获取到任务数据（所有 getJob 都失败了）
      if (tasksData.length === 0 && taskIds.length > 0) {
        throw new AppError(
          `无法获取项目 ${projectId} 的任何任务数据`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 查找下一个未完成或未审批的任务
      const nextTask = tasksData.find(task => !(task.status === "done" && task.approved));
      
      if (!nextTask) {
        // 所有任务都已完成和审批?
        const allDoneAndApproved = tasksData.every(task => task.status === "done" && task.approved);
        if (allDoneAndApproved && !projectData.completed) {
          return null; // 表示所有任务已完成和审批，等待项目完成审批
        }
        throw new AppError(
          '找不到未完成或未审批的任务',
          AppErrorCode.TaskNotFound
        );
      }
      
      return nextTask;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '获取下一个任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 列出项目
   * @param state 项目状态过滤器
   * @returns 项目列表
   */
  public async listProjects(state?: "open" | "pending_approval" | "completed" | "all"): Promise<Project[]> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      const pattern = redisKeys.projectMetadataPattern();
      console.log(`listProjects: Using pattern: ${pattern}`);
      
      // 获取所有项目ID的键
      const projectMetadataKeys = await redis.keys(pattern);
      console.log(`listProjects: Keys found (${projectMetadataKeys.length}): ${projectMetadataKeys.join(', ')}`);
      
      if (!projectMetadataKeys || projectMetadataKeys.length === 0) {
        console.log('listProjects: No project metadata keys found.');
        return [];
      }
      
      // 提取项目ID
      const regex = redisKeys.projectIdFromKeyRegex();
      console.log(`listProjects: Using regex: ${regex}`);
      const projectIds = projectMetadataKeys.map(key => {
        const match = key.match(regex);
        console.log(`listProjects: Matching key "${key}" against regex: ${match ? `Success (ID: ${match[1]})` : 'Fail'}`);
        return match ? match[1] : null;
      }).filter(Boolean) as string[];
      console.log(`listProjects: Extracted Project IDs (${projectIds.length}): ${projectIds.join(', ')}`);
      
      if (projectIds.length === 0) {
          console.warn(`listProjects: Keys found, but no project IDs extracted.`);
          return [];
      }
      
      // 获取每个项目的数据
      const projectsData = await Promise.all(
        projectIds.map(id => this.getProjectData(id))
      );
      
      // 获取每个项目的任务信息
      const projectsWithTasks = await Promise.all(
        projectsData.map(async projectData => {
          const taskIds = await redis.smembers(RedisKeys.projectTasks(projectData.projectId));
          
          if (taskIds.length === 0) {
            return {
              projectData,
              completedTasks: 0,
              approvedTasks: 0
            };
          }
          
          const queue = this.getProjectQueue(projectData.projectId);
          const jobs = await Promise.all(taskIds.map(id => this._getJobWithRetry(queue, id)));
          const tasksData = jobs.map(job => job?.data as BullMQTaskData).filter(Boolean);
          
          const completedTasks = tasksData.filter(task => task.status === "done").length;
          const approvedTasks = tasksData.filter(task => task.approved).length;
          
          return {
            projectData,
            completedTasks,
            approvedTasks
          };
        })
      );
      
      // 应用状态过滤
      let filteredProjects = projectsWithTasks;
      
      if (state && state !== "all") {
        filteredProjects = filteredProjects.filter(({ projectData, completedTasks }) => {
          switch (state) {
            case "open":
              return !projectData.completed;
            case "completed":
              return projectData.completed;
            case "pending_approval":
              return !projectData.completed && completedTasks === projectData.taskCount;
            default:
              return true;
          }
        });
      }
      
      // 转换为API返回格式
      return filteredProjects.map(({ projectData, completedTasks, approvedTasks }) => ({
        projectId: projectData.projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        tasks: [], // 这里不填充任务详情，因为listProjects通常只需要摘要
        completed: projectData.completed,
        autoApprove: projectData.autoApprove,
        ...(projectData.tenantId !== undefined && { tenantId: projectData.tenantId }),
        createdAt: projectData.createdAt ? new Date(projectData.createdAt).toISOString() : new Date().toISOString(),
        updatedAt: projectData.updatedAt ? new Date(projectData.updatedAt).toISOString() : new Date().toISOString(),
      }));
    } catch (error) {
      console.error('Error in listProjects:', error);
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '列出项目失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 获取任务数据，先尝试使用BullMQ的getJob方法，失败则尝试直接从Redis读取
   * @param queue 任务队列
   * @param taskId 任务ID
   * @returns 任务数据或null
   */
  private async _getTaskDataWithFallback(queue: Queue, taskId: string): Promise<BullMQTaskData | null> {
    // 首先尝试使用标准BullMQ方法获取任务
    const job = await this._getJobWithRetry(queue, taskId);
    
    // 如果成功获取到任务，直接返回其数据
    if (job) {
      const data = job.data as BullMQTaskData;
      
      // 确保时间戳字段存在
      if (!data.createdAt) {
        data.createdAt = Date.now();
      }
      if (!data.updatedAt) {
        data.updatedAt = Date.now();
      }
      
      return data;
    }
    
    console.log(`_getTaskDataWithFallback: Failed to get job '${taskId}' using BullMQ API, trying direct Redis access...`);
    
    // 如果标准方法失败，尝试直接从Redis读取
    return null; // 明确返回 null，让调用方处理
  }

  /**
   * 列出项目任务
   * @param projectId 项目ID
   * @param state 任务状态过滤器
   * @returns 任务列表
   */
  public async listTasks(
    projectId?: string,
    state?: "open" | "pending_approval" | "completed" | "all"
  ): Promise<Task[]> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      let allTasks: BullMQTaskData[] = [];
      
      if (projectId) {
        // 检查项目是否存在
        const currentRedisKeys = this.getRedisKeys();
        const projectMetadataKey = currentRedisKeys.projectMetadata(projectId);
        const keyType = await redis.type(projectMetadataKey);
        if (keyType !== 'hash') {
          console.warn(`Project check failed for key: ${projectMetadataKey}. Expected 'hash', got '${keyType}'.`);
          throw new AppError(
            `项目 ${projectId} 不存在或元数据无效`,
            AppErrorCode.ProjectNotFound
          );
        }
        
        // 获取特定项目的所有任务 ID
        const taskIds = await redis.smembers(currentRedisKeys.projectTasks(projectId));
        
        if (!taskIds || taskIds.length === 0) {
          console.log(`listTasks: No tasks found for project '${projectId}'`);
          return [];
        }
        
        const queue = this.getProjectQueue(projectId);
        
        // 使用 _getJobWithRetry 获取 Job 对象
        const jobPromises = taskIds.map(id => this._getJobWithRetry(queue, id));
        const jobs = await Promise.all(jobPromises);
        
        // 提取任务数据，过滤掉 null (获取失败的 Job)
        allTasks = jobs.filter(job => job !== null).map(job => job!.data as BullMQTaskData);
        
        if (allTasks.length < taskIds.length) {
          console.warn(`listTasks: Retrieved ${allTasks.length} tasks out of ${taskIds.length} task IDs for project '${projectId}'. Some jobs might not be available yet or failed to retrieve.`);
        }
      } else {
        // 获取所有项目
        const projects = await this.listProjects();
        
        // 递归获取所有项目的任务
        for (const project of projects) {
          const projectTasks = await this.listTasks(project.projectId); 
          allTasks = [...allTasks, ...projectTasks.map(t => ({
            id: t.id,
            title: t.title,
            description: t.description,
            status: t.status,
            approved: t.approved,
            completedDetails: t.completedDetails,
            toolRecommendations: t.toolRecommendations,
            ruleRecommendations: t.ruleRecommendations,
            projectId: project.projectId,
            tenantId: t.tenantId,
            createdAt: t.createdAt ? new Date(t.createdAt).getTime() : Date.now(),
            updatedAt: t.updatedAt ? new Date(t.updatedAt).getTime() : Date.now()
          } as BullMQTaskData))];
        }
      }
      
      // 应用状态过滤
      if (state && state !== "all") {
        allTasks = allTasks.filter(task => {
          switch (state) {
            case "open":
              return !task.approved;
            case "completed":
              return task.status === "done" && task.approved;
            case "pending_approval":
              return task.status === "done" && !task.approved;
            default:
              return true;
          }
        });
      }
      
      // 转换为Task接口格式
      return allTasks.map(taskData => ({
        id: taskData.id,
        title: taskData.title,
        description: taskData.description,
        status: taskData.status,
        approved: taskData.approved,
        completedDetails: taskData.completedDetails,
        toolRecommendations: taskData.toolRecommendations,
        ruleRecommendations: taskData.ruleRecommendations,
        ...(taskData.tenantId !== undefined && { tenantId: taskData.tenantId }),
        createdAt: taskData.createdAt ? new Date(taskData.createdAt).toISOString() : new Date().toISOString(),
        updatedAt: taskData.updatedAt ? new Date(taskData.updatedAt).toISOString() : new Date().toISOString()
      }));
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '列出任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 删除任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @returns 成功消息
   */
  public async deleteTask(projectId: string, taskId: string): Promise<string> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 检查项目是否存在
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      const projectExists = await redis.exists(projectMetadataKey);
      if (projectExists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      // 获取项目数据
      const projectData = await this.getProjectData(projectId);
      if (projectData.completed) {
        throw new AppError(
          '项目已完成，无法删除任务',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      // 检查任务是否存在于项目中
      const projectTasksKey = redisKeys.projectTasks(projectId);
      const isMember = await redis.sismember(projectTasksKey, taskId);
      if (!isMember) {
        throw new AppError(
          `任务 ${taskId} 不存在于项目 ${projectId} 中`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取队列
      const queue = this.getProjectQueue(projectId);
      
      // 尝试获取 Job 对象以检查审批状态并删除
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (job) {
        const taskData = job.data as BullMQTaskData;
        // 检查任务是否已审批
        if (taskData.approved) {
          throw new AppError(
            '无法删除已审批的任务',
            AppErrorCode.CannotModifyApprovedTask
          );
        }
        // 删除任务
        await job.remove();
      } else {
        // 如果找不到 Job 对象，可能任务已被删除或获取失败，但仍尝试从集合中移除
        console.warn(`deleteTask: BullMQ Job object for task ${taskId} not found, attempting to remove from project set.`);
      }
      
      // 从项目任务集合中移除 (无论是否找到 Job 对象)
      const removedCount = await redis.srem(projectTasksKey, taskId);
      
      // 如果从集合中成功移除，则更新计数器
      if (removedCount > 0) {
        await redis.hincrby(projectMetadataKey, 'taskCount', -1);
      }
      
      return `已从项目 ${projectId} 中删除任务 ${taskId}`;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '删除任务失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 删除项目
   * @param projectId 项目ID
   * @returns 成功消息
   */
  public async deleteProject(projectId: string): Promise<string> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 检查项目是否存在
      const projectMetadataKey = redisKeys.projectMetadata(projectId);
      const projectExists = await redis.exists(projectMetadataKey);
      if (projectExists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      // 注销项目的Worker
      try {
        // 仅当WorkerManager已初始化时才注销Worker
        if (this.workerManager.isInitialized()) {
          await this.workerManager.unregister(projectId);
        }
      } catch (error) {
        console.warn(`注销项目 ${projectId} 的Worker失败:`, error);
        // 继续删除项目，但记录警告
      }
      
      // 获取项目任务列表
      const projectTasksKey = redisKeys.projectTasks(projectId);
      const taskIds = await redis.smembers(projectTasksKey);
      const queue = this.getProjectQueue(projectId);
      
      // 删除项目中的所有任务
      for (const taskId of taskIds) {
        // 获取任务
        const job = await this._getJobWithRetry(queue, taskId);
        
        if (job) {
          // 删除任务
          await job.remove();
        } else {
          console.warn(`deleteProject: BullMQ job object for task ${taskId} not found, will remove from project set only.`);
        }
        
        // 从项目任务集合中移除
        await redis.srem(projectTasksKey, taskId);
      }
      
      // 删除项目元数据
      await redis.del(projectMetadataKey);
      
      // 删除项目任务集合
      await redis.del(projectTasksKey);
      
      // 尝试删除队列
      try {
        // 使用 redisKeys 生成的正确队列名
        const queueName = redisKeys.projectQueueName(projectId); 
        const queueInstance = this.queues.get(queueName);

        // 在清除前关闭队列实例（如果存在）
        if (queueInstance) {
            await queueInstance.close(); 
            // 调用队列实例的 obliterate 方法
            await queueInstance.obliterate({ force: true }); // 添加 force 选项以彻底清理
            this.queues.delete(queueName); // 只有在成功清除后才从映射中移除
        } else {
            // 如果队列实例不在映射中，只记录警告
            console.warn(`deleteProject: Queue instance for ${queueName} not found in map, cannot obliterate via instance. Associated Redis keys might remain.`);
            // 如果无论如何都添加到了映射中，尝试移除
            if(this.queues.has(queueName)) { 
              this.queues.delete(queueName);
            }
        }
        
        // 从 Bull Board 中移除队列（无论队列清除是否成功）
        try {
          // 传递当前前缀以确保移除正确的队列
          await removeQueueFromBoard(projectId, this.getCurrentPrefix()); 
        } catch (error) {
          // 即使从 Bull Board 移除失败，我们仍然继续操作
          console.warn(`从 Bull Board 移除队列 ${projectId} 时出错，但不影响项目删除:`, error);
        }
      } catch (error) {
        console.warn(`清理项目队列时出错: ${error}`);
        // 即使清理队列失败，我们仍然继续删除项目
      }
      
      return `已删除项目 ${projectId}`;
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '删除项目失败',
        AppErrorCode.JobProcessingError,
        error
      );
    }
  }

  /**
   * 读取项目
   * @param projectId 项目ID
   * @returns 项目数据及其任务
   */
  public async readProject(projectId: string): Promise<Project> {
    this.ensureReady();
    
    try {
      const redis = this.redisManager.getConnection();
      const redisKeys = this.getRedisKeys();
      
      // 获取项目数据 (BullMQProjectData)
      const projectData = await this.getProjectData(projectId);
      
      // 获取项目的所有任务 - 使用 listTasks 确保使用相同的任务获取逻辑
      const tasks = await this.listTasks(projectId);
      
      return {
        projectId: projectData.projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        tasks,
        completed: projectData.completed,
        autoApprove: projectData.autoApprove,
        taskCount: projectData.taskCount, 
        projectConclusion: projectData.projectConclusion, // 确保映射 projectConclusion
        ...(projectData.tenantId !== undefined && { tenantId: projectData.tenantId }),
        createdAt: projectData.createdAt ? new Date(projectData.createdAt).toISOString() : new Date().toISOString(),
        updatedAt: projectData.updatedAt ? new Date(projectData.updatedAt).toISOString() : new Date().toISOString()
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '读取项目失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 关闭BullMQ服务
   */
  public async close(): Promise<void> {
    if (this.serviceState === BullMQServiceState.CLOSED) {
      return;
    }
    
    try {
      // 关闭所有工作进程
      if (this.workerManager.isInitialized()) {
        await this.workerManager.shutdownAll();
      }
      
      // 关闭所有队列
      for (const [name, queue] of this.queues.entries()) {
        await queue.close();
      }
      
      // 关闭所有工作进程
      for (const [name, worker] of this.workers.entries()) {
        await worker.close();
      }
      
      // 关闭所有队列事件监听器
      for (const [name, queueEvents] of this.queueEvents.entries()) {
        await queueEvents.close();
      }
      
      // 关闭Flow Producer
      if (this.flowProducer) {
        await this.flowProducer.close();
      }
      
      // 关闭Redis连接
      await this.redisManager.close();
      
      this.serviceState = BullMQServiceState.CLOSED;
    } catch (error) {
      console.error('关闭BullMQ服务失败:', error);
      throw new AppError(
        '关闭BullMQ服务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 将对象扁平化为键值对
   * 用于将对象转换为Redis哈希表格式
   * @param obj 输入对象
   * @returns 扁平化的键值对
   */
  private flattenObject(obj: Record<string, any>): Record<string, string> {
    const result: Record<string, string> = {};
    
    for (const [key, value] of Object.entries(obj)) {
      if (typeof value === 'object' && value !== null) {
        result[key] = JSON.stringify(value);
      } else if (typeof value === 'boolean') {
        result[key] = value ? 'true' : 'false';
      } else {
        result[key] = String(value);
      }
    }
    
    return result;
  }

  /**
   * 设置队列前缀，用于多租户隔离
   * @param prefix 新的队列前缀
   */
  public setPrefix(prefix?: string): void {
    // 如果前缀没有变化，则不做任何操作
    if (this.options.prefix === prefix) {
      return;
    }
    
    // 规范化前缀格式 - 使用RedisNamingValidator
    const normalizedPrefix = normalizeRedisPrefix(prefix);
    
    // 记录前缀变更
    if (prefix && normalizedPrefix !== prefix) {
      console.warn(`规范化前缀: ${prefix} -> ${normalizedPrefix}`);
    }
    
    console.log(`更改前缀: 从 '${this.options.prefix || "无"}' 到 '${normalizedPrefix || "无"}'`);
    
    // 保存旧前缀用于日志记录
    const oldPrefix = this.options.prefix;
    
    // 更新前缀设置
    this.options.prefix = normalizedPrefix;
    
    // 更新WorkerManager的前缀设置
    this.workerManager.setPrefix(normalizedPrefix);
    
    // 关闭并重新创建现有队列，使用新前缀
    if (this.serviceState === BullMQServiceState.READY) {
      // 记录当前所有队列的名称
      const queueNames = Array.from(this.queues.keys());
      
      // 关闭所有现有队列
      const closePromises = [];
      for (const [name, queue] of this.queues.entries()) {
        console.log(`关闭队列: ${name}`);
        closePromises.push(queue.close());
      }
      // Wait for all queues to close before clearing the map
      Promise.all(closePromises).catch(err => {
           console.error('Error closing queues during prefix change:', err);
      });

      // 清空队列缓存
      this.queues.clear();
      console.log(`已清空 ${queueNames.length} 个队列缓存`);
      
      // 清除flowProducer并使用新前缀重新创建
      if (this.flowProducer) {
        this.flowProducer.close().catch(err => {
          console.error('关闭FlowProducer时出错:', err);
        });
        
        this.flowProducer = new FlowProducer({
          connection: this.redisManager.getConnection(),
          // Keep prefix empty here as well
          prefix: ''  
        });
      }
      
      // 使用新的前缀读取计数器
      this.loadCounters().catch(err => {
        console.error('重新加载计数器失败:', err);
      });
    }
  }
} 