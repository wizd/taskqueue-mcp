import { Worker, Job, WorkerOptions } from 'bullmq';
import { BullMQTaskData, createRedisKeys, normalizeRedisPrefix } from '../types/bullmq.js';
import { RedisManager } from './RedisManager.js';
import { RedisOptions } from 'ioredis';
import { Logger } from './Logger.js';
import { experimental_createMCPClient, generateText } from 'ai';
import { Project, Task } from '../types/data.js';
import { modelProvider } from '../../lib/ai/provider.js';
import { createStreamHttpClient } from '../../lib/mcp/streamHttpClient.js';

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

      // create mcp client
      let mcpClientsToClose: Awaited<
        ReturnType<typeof experimental_createMCPClient>
      >[] = [];

      const ytdlp_url = process.env.YTDLP_MCP_URL;
      const ytdlp_api_key = process.env.YTDLP_MCP_API_KEY;
      if (!ytdlp_url || !ytdlp_api_key) {
        throw new Error('YTDLP_MCP_URL 或 YTDLP_MCP_API_KEY 未配置');
      }
      const mcpClient = await createStreamHttpClient("", ytdlp_url, ytdlp_api_key);
      mcpClientsToClose.push(mcpClient);

      const vidgen_url = process.env.VID_GEN_MCP_URL;
      const vidgen_api_key = process.env.VID_GEN_MCP_API_KEY;
      if (!vidgen_url || !vidgen_api_key) {
        throw new Error('VID_GEN_MCP_URL 或 VID_GEN_MCP_API_KEY 未配置');
      }
      const vidgenClient = await createStreamHttpClient("", vidgen_url, vidgen_api_key);
      mcpClientsToClose.push(vidgenClient);

      const mcpTools = await mcpClient.tools();
      const vidgenTools = await vidgenClient.tools();
      const tools = {
        ...mcpTools,
        ...vidgenTools,
        //getWeather,
      };
      console.log('combined tools is ', tools);

      let projectContextString = "项目核心上下文不可用或获取失败。";
      if (this.readProjectFunction) {
        try {
          this.logger.info(`正在为任务 ${taskData.id} 获取项目 ${projectId} 的核心上下文...`);
          const projectContext: Project = await this.readProjectFunction(projectId);
          // 为了LLM提示，我们在这里只序列化项目本身，避免循环引用或过大的上下文
          const projectInfoForPrompt = { 
            projectId: projectContext.projectId,
            initialPrompt: projectContext.initialPrompt,
            projectPlan: projectContext.projectPlan,
            completed: projectContext.completed,
            autoApprove: projectContext.autoApprove,
            taskCount: projectContext.taskCount,
            // 不在此处包含 projectContext.tasks 或 projectContext.projectConclusion
            createdAt: projectContext.createdAt,
            updatedAt: projectContext.updatedAt
          };
          projectContextString = JSON.stringify(projectInfoForPrompt, null, 2);
          this.logger.info(`成功获取项目 ${projectId} 的核心上下文 (任务 ${taskData.id})`);
          await job.updateProgress(20);
        } catch (e) {
          this.logger.error(`获取项目 ${projectId} 核心上下文失败 (任务 ${taskData.id}):`, e);
          projectContextString = `获取项目核心上下文失败: ${e instanceof Error ? e.message : String(e)}`;
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
`你好！在开始之前，请了解你可以使用以下工具来协助完成任务：

<available_tools>
- 视频下载工具：可下载数千个视频网站的视频、音频、字幕等数据。
- FFmpeg执行工具：一个可执行任意FFmpeg命令的工具，用于音视频和图片的编辑剪辑。
- 素材生成工具：基于Google Gemini，可生成图片和视频素材。
</available_tools>

请注意：除了上述明确列出的工具，所有其他的思考、分析、决策和执行步骤都需要由你独立完成。

接下来，这里有一些关于当前项目的背景信息，以及一个需要你协助处理的具体任务。请先仔细阅读这些材料。

<project_context>
${projectContextString}
</project_context>

然后，这是你需要处理的具体任务：
<current_task>
ID: ${taskData.id}
Title: ${taskData.title}
Description: ${taskData.description}
Status: ${taskData.status}
Approved: ${taskData.approved}
${taskData.toolRecommendations ? `Tool Recommendations: ${taskData.toolRecommendations}` : ''}
${taskData.ruleRecommendations ? `Rule Recommendations: ${taskData.ruleRecommendations}` : ''}
</current_task>

现在，请你基于上述所有信息，像在平时对话那样，告诉我你将如何完成这项任务。请详细描述你的思考过程、计划采取的步骤、关键的观察点（包括何时以及如何使用上述工具），以及预期的任务成果或结论。
你的回复将被直接用作该任务的"完成详情"（completedDetails）记录下来。
因此，请确保你的表述清晰、完整，并且紧扣任务的要求。谢谢！`;
      
      await job.updateProgress(30);

      let llmResultText = "LLM处理被跳过或遇到问题。使用默认完成详情。";
      try {
        this.logger.info(`开始为任务 ${taskData.id} 调用LLM...`);
      
        const { text: generatedText } = await generateText({
            model: modelProvider,
            prompt: llmPrompt,
            tools,
            maxSteps: 10,
            onStepFinish: async (step) => {
              console.log('onStepFinish', step);
            }
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

      // 检查项目是否所有任务都已完成，并执行项目总结（如果需要）
      await this.checkAndConcludeProject(projectId);
      
      console.log(
        `Closing ${mcpClientsToClose.length} MCP clients in onFinish...`,
      );
      for (const client of mcpClientsToClose) {
        try {
          await client.close();
        } catch (closeError: unknown) {
          console.error(
            'Error closing MCP client in onFinish:',
            closeError,
          );
        }
      }
      mcpClientsToClose = [];

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

  private async checkAndConcludeProject(projectId: string): Promise<void> {
    if (!this.readProjectFunction) {
      this.logger.warn(`[ProjectConclude] 未提供 readProjectFunction，无法检查项目 ${projectId} 是否完成。`);
      return;
    }
    if (!this.finalizeProjectFunction) {
        this.logger.warn(`[ProjectConclude] 未提供 finalizeProjectFunction，无法完成项目 ${projectId}。`);
        return;
    }

    try {
      this.logger.info(`[ProjectConclude] 检查项目 ${projectId} 是否所有任务都已完成...`);
      const project = await this.readProjectFunction(projectId);

      if (project.completed) {
        this.logger.info(`[ProjectConclude] 项目 ${projectId} 已经标记为完成，跳过总结。`);
        return;
      }

      const allTasksDone = project.tasks && project.tasks.every(task => task.status === "done");

      if (allTasksDone && project.tasks.length > 0) { // 确保有任务且所有任务都完成
        this.logger.info(`[ProjectConclude] 项目 ${projectId} 所有任务均已完成，开始执行项目总结。`);
        await this.concludeProject(projectId, project); // 传递 project 对象以避免再次读取
      } else {
        if (project.tasks.length === 0) {
             this.logger.info(`[ProjectConclude] 项目 ${projectId} 没有任务，考虑直接标记完成或通过其他流程处理。当前跳过自动总结。`);
        } else {
            this.logger.info(`[ProjectConclude] 项目 ${projectId} 尚有未完成的任务，不执行项目总结。`);
        }
      }
    } catch (error) {
      this.logger.error(`[ProjectConclude] 检查或执行项目 ${projectId} 总结时出错:`, error);
    }
  }

  private async concludeProject(projectId: string, projectData: Project): Promise<void> {
    this.logger.info(`[ConcludeProject] 开始为项目 ${projectId} 生成总结...`);

    // 准备项目所有任务的详情字符串
    let tasksDetailsString = projectData.tasks.map(task => 
      `任务ID: ${task.id}\n标题: ${task.title}\n状态: ${task.status}\n审批状态: ${task.approved}\n完成详情: ${task.completedDetails || '无'}\n---`
    ).join('\n\n');
    if (!tasksDetailsString) tasksDetailsString = "该项目没有任务或未能获取任务详情。";

    const projectContextForLLM = JSON.stringify({ 
        projectId: projectData.projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        autoApprove: projectData.autoApprove,
        taskCount: projectData.tasks.length,
        createdAt: projectData.createdAt,
        updatedAt: projectData.updatedAt,
     }, null, 2);

    const llmPrompt = 
`你好！以下是一个项目的完整上下文信息，包括其所有任务的处理结果。

<project_overview>
${projectContextForLLM}
</project_overview>

<all_tasks_details>
${tasksDetailsString}
</all_tasks_details>

现在，所有任务均已处理完毕。请你基于以上所有信息，为整个项目撰写一份最终的总结报告。
这份报告应该概述项目的主要成果、遇到的挑战（如果有）、关键的学习点以及项目的整体完成情况。
你的回复将作为项目的最终总结（projectConclusion）被保存下来。
请确保内容全面、精炼，并能准确反映项目的整个生命周期。谢谢！`;

    let projectLlmConclusion = "LLM项目总结失败或被跳过。";
    try {
      this.logger.info(`[ConcludeProject] 调用LLM为项目 ${projectId} 生成总结...`);

      const { text: generatedConclusion } = await generateText({
        model: modelProvider,
        prompt: llmPrompt,
      });
      projectLlmConclusion = generatedConclusion;
      this.logger.info(`[ConcludeProject] LLM为项目 ${projectId} 生成总结成功。`);
    } catch (llmError) {
      this.logger.error(`[ConcludeProject] LLM为项目 ${projectId} 生成总结失败:`, llmError);
      projectLlmConclusion = `LLM项目总结失败: ${llmError instanceof Error ? llmError.message : String(llmError)}`;
    }

    if (this.finalizeProjectFunction) {
      try {
        this.logger.info(`[ConcludeProject] 调用 finalizeProjectFunction 保存项目 ${projectId} 的总结并标记完成...`);
        await this.finalizeProjectFunction(projectId, projectLlmConclusion);
        this.logger.info(`[ConcludeProject] 项目 ${projectId} 总结已保存并标记为完成。`);
      } catch (finalizeError) {
        this.logger.error(`[ConcludeProject] 调用 finalizeProjectFunction 完成项目 ${projectId} 时出错:`, finalizeError);
      }
    } else {
      this.logger.error(`[ConcludeProject] finalizeProjectFunction 未定义，无法完成项目 ${projectId}。`);
    }
  }
} 