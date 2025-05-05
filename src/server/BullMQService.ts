import { Queue, Worker, QueueEvents, Job, FlowProducer } from 'bullmq';
import { Redis } from 'ioredis';
import { AppError, AppErrorCode } from '../types/errors.js';
import { RedisManager } from './RedisManager.js';
import { BullMQServiceOptions, BullMQServiceState, RedisKeys, BullMQTaskData, BullMQProjectData } from '../types/bullmq.js';
import { Task, Project } from '../types/data.js';

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

  /**
   * 创建BullMQService实例
   * @param options BullMQ服务配置选项
   */
  constructor(options: BullMQServiceOptions = {}) {
    this.options = options;
    this.redisManager = RedisManager.getInstance(options.connection);
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
        prefix: this.options.prefix
      });

      // 加载计数器
      await this.loadCounters();
      
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
      
      // 获取项目计数器，如果不存在则初始化为0
      const projectCounter = await redis.get(RedisKeys.projectCounter);
      this.projectCounter = projectCounter ? parseInt(projectCounter, 10) : 0;
      
      // 获取任务计数器，如果不存在则初始化为0
      const taskCounter = await redis.get(RedisKeys.taskCounter);
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
   * 获取项目队列
   * @param projectId 项目ID
   * @returns 项目队列实例
   */
  public getProjectQueue(projectId: string): Queue {
    this.ensureReady();
    
    const queueName = RedisKeys.projectQueueName(projectId);
    if (!this.queues.has(queueName)) {
      const queue = new Queue(queueName, {
        connection: this.redisManager.getConnection(),
        prefix: this.options.prefix,
        ...this.options.projectQueueOptions
      });
      this.queues.set(queueName, queue);
    }
    
    return this.queues.get(queueName)!;
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
      
      // 增加项目计数器
      this.projectCounter = await redis.incr(RedisKeys.projectCounter);
      const projectId = `proj-${this.projectCounter}`;
      
      // 创建项目元数据
      const projectData: BullMQProjectData = {
        projectId,
        initialPrompt,
        projectPlan: projectPlan || initialPrompt,
        completed: false,
        autoApprove,
        taskCount: 0
      };
      
      // 存储项目元数据到Redis哈希表
      await redis.hset(
        RedisKeys.projectMetadata(projectId),
        this.flattenObject(projectData)
      );
      
      // 确保创建项目队列
      this.getProjectQueue(projectId);
      
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
      
      // 检查项目是否存在
      const exists = await redis.exists(RedisKeys.projectMetadata(projectId));
      if (exists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      // 获取项目元数据
      const projectData = await this.getProjectData(projectId);
      if (projectData.completed) {
        throw new AppError(
          '项目已完成，无法添加任务',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      const queue = this.getProjectQueue(projectId);
      const taskIds: string[] = [];
      
      // 添加每个任务到队列
      for (const taskDef of tasks) {
        this.taskCounter = await redis.incr(RedisKeys.taskCounter);
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
          projectId
        };
        
        // 添加任务到项目队列
        await queue.add(taskId, taskData, {
          ...this.options.defaultJobOptions
        });
        
        // 添加任务ID到项目任务集合
        await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
        
        // --- 强制增加延迟 --- 
        await new Promise(resolve => setTimeout(resolve, 100)); // 增加 100ms 延迟
        // --- 结束延迟 --- 

        // --- 临时调试/确认步骤 ---
        const memberExists = await redis.sismember(RedisKeys.projectTasks(projectId), taskId);
        if (!memberExists) {
            console.error(`!!! CRITICAL: Task ${taskId} was NOT added to Redis set for project ${projectId} immediately after sadd.`);
            // 可以在这里抛出错误，或者至少记录下来
            // throw new AppError(`Failed to reliably add task ${taskId} to project set ${projectId}`, AppErrorCode.RedisCommandError);
        }
        // --- 结束调试步骤 ---

        taskIds.push(taskId);
      }
      
      // 更新项目任务计数
      await redis.hincrbyfloat(
        RedisKeys.projectMetadata(projectId),
        'taskCount',
        tasks.length
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
      
      const projectData = await redis.hgetall(RedisKeys.projectMetadata(projectId));
      if (!projectData || Object.keys(projectData).length === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      return {
        projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        completed: projectData.completed === 'true',
        autoApprove: projectData.autoApprove === 'true',
        taskCount: parseInt(projectData.taskCount || '0', 10)
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
      
      return job.data as BullMQTaskData;
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
      const queue = this.getProjectQueue(projectId);
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (!job) {
        throw new AppError(
          `任务 ${taskId} 不存在`,
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
      
      // 应用更新
      const updatedData: BullMQTaskData = {
        ...taskData,
        ...updates
      };
      
      // 更新任务数据
      await job.updateData(updatedData);
      
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
      const queue = this.getProjectQueue(projectId);
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (!job) {
        throw new AppError(
          `任务 ${taskId} 不存在`,
          AppErrorCode.TaskNotFound
        );
      }
      
      const taskData = job.data as BullMQTaskData;
      
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
      
      // 更新审批状态
      const updatedData: BullMQTaskData = {
        ...taskData,
        approved: true
      };
      
      // 更新任务数据
      await job.updateData(updatedData);
      
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
      
      // 获取项目数据
      const projectData = await this.getProjectData(projectId);
      
      if (projectData.completed) {
        throw new AppError(
          '项目已完成',
          AppErrorCode.ProjectAlreadyCompleted
        );
      }
      
      // 获取所有任务
      const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
      const queue = this.getProjectQueue(projectId);
      
      // 检查所有任务是否已完成和审批
      const jobs = await Promise.all(taskIds.map(id => this._getJobWithRetry(queue, id)));
      const tasksData = jobs.map(job => job?.data as BullMQTaskData).filter(Boolean);
      
      // 增加一个检查，确保所有任务都成功获取到了
      if (tasksData.length !== taskIds.length) {
        const missingIds = taskIds.filter(id => !jobs.some(j => j?.id === id));
        console.warn(`approveProjectCompletion: Could not retrieve jobs for tasks: ${missingIds.join(', ')} in project ${projectId}`);
        // 抛出错误可能更合适，因为无法确认所有任务状态
        throw new AppError(
          `无法获取项目 ${projectId} 的所有任务状态`,
          AppErrorCode.TaskNotFound // 或者定义一个更具体的错误码
        );
      }
      
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
      
      // 更新项目为已完成
      await redis.hset(
        RedisKeys.projectMetadata(projectId),
        'completed',
        'true'
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
      
      // 获取所有任务ID
      const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
      
      if (taskIds.length === 0) {
        throw new AppError(
          '项目没有任务',
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取所有任务的Job
      const jobs = await Promise.all(taskIds.map(id => this._getJobWithRetry(queue, id)));
      const tasksData = jobs.map(job => job?.data as BullMQTaskData).filter(Boolean);
      
      // 增加一个检查，确保所有任务都成功获取到了
      if (tasksData.length !== taskIds.length) {
         const missingIds = taskIds.filter(id => !jobs.some(j => j?.id === id));
         console.warn(`getNextTask: Could not retrieve jobs for tasks: ${missingIds.join(', ')} in project ${projectId}`);
         // 如果找不到所有任务，可能无法确定下一个任务，可以抛错或返回null/错误
         throw new AppError(
           `无法获取项目 ${projectId} 的所有任务状态以确定下一个任务`,
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
      
      // 获取所有项目ID的键
      const projectMetadataKeys = await redis.keys(`project:proj-*:metadata`);
      
      if (!projectMetadataKeys || projectMetadataKeys.length === 0) {
        return [];
      }
      
      // 提取项目ID
      const projectIds = projectMetadataKeys.map(key => {
        const match = key.match(/project:(proj-\d+):metadata/);
        return match ? match[1] : null;
      }).filter(Boolean) as string[];
      
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
        autoApprove: projectData.autoApprove
      }));
    } catch (error) {
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
        const exists = await redis.exists(RedisKeys.projectMetadata(projectId));
        if (exists === 0) {
          throw new AppError(
            `项目 ${projectId} 不存在`,
            AppErrorCode.ProjectNotFound
          );
        }
        
        // 获取特定项目的所有任务
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        const queue = this.getProjectQueue(projectId);
        const jobs = await Promise.all(taskIds.map(id => this._getJobWithRetry(queue, id)));
        allTasks = jobs.map(job => job?.data as BullMQTaskData).filter(Boolean);
      } else {
        // 获取所有项目
        const projects = await this.listProjects();
        
        // 获取所有项目的所有任务
        for (const project of projects) {
          const taskIds = await redis.smembers(RedisKeys.projectTasks(project.projectId));
          const queue = this.getProjectQueue(project.projectId);
          const jobs = await Promise.all(taskIds.map(id => this._getJobWithRetry(queue, id)));
          const projectTasks = jobs.map(job => job?.data as BullMQTaskData).filter(Boolean);
          allTasks = [...allTasks, ...projectTasks];
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
        ruleRecommendations: taskData.ruleRecommendations
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
      
      // 检查项目是否存在
      const projectExists = await redis.exists(RedisKeys.projectMetadata(projectId));
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
      const isMember = await redis.sismember(RedisKeys.projectTasks(projectId), taskId);
      if (!isMember) {
        throw new AppError(
          `任务 ${taskId} 不存在于项目 ${projectId} 中`,
          AppErrorCode.TaskNotFound
        );
      }
      
      // 获取任务详情
      const queue = this.getProjectQueue(projectId);
      const job = await this._getJobWithRetry(queue, taskId);
      
      if (!job) {
        // 即使 sismember 返回 true，getJob 也可能失败（理论上不应该，但增加健壮性）
         throw new AppError(
           `任务 ${taskId} 存在于集合但无法获取 Job 对象`,
           AppErrorCode.TaskNotFound
         );
      }
      
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
      
      // 从项目任务集合中移除
      await redis.srem(RedisKeys.projectTasks(projectId), taskId);
      
      // 更新项目任务计数
      await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', -1);
      
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
      
      // 检查项目是否存在
      const projectExists = await redis.exists(RedisKeys.projectMetadata(projectId));
      if (projectExists === 0) {
        throw new AppError(
          `项目 ${projectId} 不存在`,
          AppErrorCode.ProjectNotFound
        );
      }
      
      // 获取项目任务列表
      const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
      const queue = this.getProjectQueue(projectId);
      
      // 删除项目中的所有任务
      for (const taskId of taskIds) {
        // 获取任务
        const job = await this._getJobWithRetry(queue, taskId);
        
        if (job) {
          // 删除任务
          await job.remove();
        }
        
        // 从项目任务集合中移除
        await redis.srem(RedisKeys.projectTasks(projectId), taskId);
      }
      
      // 删除项目元数据
      await redis.del(RedisKeys.projectMetadata(projectId));
      
      // 删除项目任务集合
      await redis.del(RedisKeys.projectTasks(projectId));
      
      // 尝试删除队列
      try {
        await queue.obliterate();
        this.queues.delete(RedisKeys.projectQueueName(projectId));
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
      
      // 获取项目数据
      const projectData = await this.getProjectData(projectId);
      
      // 获取项目的所有任务
      const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
      const tasks = await this.listTasks(projectId);
      
      return {
        projectId: projectData.projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        tasks,
        completed: projectData.completed,
        autoApprove: projectData.autoApprove
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
    this.options.prefix = prefix;
    
    // 关闭并重新创建现有队列，使用新前缀
    if (this.serviceState === BullMQServiceState.READY) {
      // 清除队列缓存，这样下一次获取队列时会使用新前缀创建
      this.queues.clear();
      
      // 清除flowProducer并使用新前缀重新创建
      if (this.flowProducer) {
        this.flowProducer.close().catch(err => {
          console.error('关闭FlowProducer时出错:', err);
        });
        
        this.flowProducer = new FlowProducer({
          connection: this.redisManager.getConnection(),
          prefix: this.options.prefix
        });
      }
    }
  }
} 