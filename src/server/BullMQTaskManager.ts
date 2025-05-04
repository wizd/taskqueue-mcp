import * as path from "node:path";
import {
  Task,
  TaskState,
  Project
} from "../types/data.js";
import {
  ProjectCreationSuccessData,
  ApproveTaskSuccessData,
  ApproveProjectSuccessData,
  OpenTaskSuccessData,
  ListProjectsSuccessData,
  ListTasksSuccessData,
  AddTasksSuccessData,
  DeleteTaskSuccessData,
  ReadProjectSuccessData,
  UpdateTaskSuccessData
} from "../types/response.js";
import { AppError, AppErrorCode } from "../types/errors.js";
import { generateObject, jsonSchema } from "ai";
import { BullMQService } from "./BullMQService.js";
import { TaskManagerBase } from "./TaskManagerBase.js";
import { BullMQTaskData, BullMQServiceOptions } from "../types/bullmq.js";

/**
 * BullMQ 任务管理器
 * 实现基于BullMQ的任务管理功能
 */
export class BullMQTaskManager extends TaskManagerBase {
  private bullMQService: BullMQService;
  private initialized: Promise<void>;

  /**
   * 创建BullMQTaskManager实例
   * @param options BullMQ服务配置选项
   */
  constructor(options?: BullMQServiceOptions) {
    super();
    this.bullMQService = new BullMQService(options);
    this.initialized = Promise.resolve();
  }

  /**
   * 确保管理器已初始化
   */
  protected async ensureInitialized(): Promise<void> {
    try {
      await this.initialized;
    } catch (error) {
      throw new AppError(
        '初始化任务管理器失败',
        AppErrorCode.ServiceNotReadyError,
        error
      );
    }
  }

  /**
   * 从磁盘重新加载数据
   * 对于BullMQ实现，此方法不执行任何操作
   */
  public async reloadFromDisk(): Promise<void> {
    // BullMQ不需要从磁盘重新加载
    return Promise.resolve();
  }

  /**
   * 创建新项目
   * @param initialPrompt 初始提示
   * @param tasks 任务列表
   * @param projectPlan 项目计划
   * @param autoApprove 是否自动审批
   */
  public async createProject(
    initialPrompt: string,
    tasks: { title: string; description: string; toolRecommendations?: string; ruleRecommendations?: string }[],
    projectPlan?: string,
    autoApprove?: boolean
  ): Promise<ProjectCreationSuccessData> {
    await this.ensureInitialized();
    
    try {
      // 创建项目
      const projectId = await this.bullMQService.createProject(
        initialPrompt,
        projectPlan,
        autoApprove === false ? false : true
      );
      
      // 添加任务
      const taskIds = await this.bullMQService.addTasksToProject(projectId, tasks);
      
      // 构建任务摘要
      const taskSummaries = tasks.map((task, index) => ({
        id: taskIds[index],
        title: task.title,
        description: task.description
      }));
      
      return {
        projectId,
        totalTasks: tasks.length,
        tasks: taskSummaries,
        message: `项目 ${projectId} 已创建，包含 ${tasks.length} 个任务。`
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        'Failed to create project',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 生成项目计划
   * @param options 参数选项
   */
  public async generateProjectPlan({
    prompt,
    provider,
    model,
    attachments,
  }: {
    prompt: string;
    provider: string;
    model: string;
    attachments: string[];
  }): Promise<ProjectCreationSuccessData> {
    await this.ensureInitialized();

    // 此部分代码与原始TaskManager中的generateProjectPlan几乎相同，
    // 主要区别在于最后使用BullMQTaskManager的createProject方法

    // 定义项目计划输出接口
    interface ProjectPlanOutput {
      projectPlan: string;
      tasks: Array<{
        title: string;
        description: string;
        toolRecommendations?: string;
        ruleRecommendations?: string;
      }>;
    }

    // 读取所有附件文件
    const attachmentContents: string[] = [];
    for (const filename of attachments) {
      try {
        const content = await this.readAttachmentFile(filename);
        attachmentContents.push(content);
      } catch (error) {
        throw new AppError(`Failed to read attachment file: ${filename}`, AppErrorCode.FileReadError, error);
      }
    }

    // 定义LLM响应的schema
    const projectPlanSchema = jsonSchema<ProjectPlanOutput>({
      type: "object",
      properties: {
        projectPlan: { type: "string" },
        tasks: {
          type: "array",
          items: {
            type: "object",
            properties: {
              title: { type: "string" },
              description: { type: "string" },
              toolRecommendations: { type: "string" },
              ruleRecommendations: { type: "string" },
            },
            required: ["title", "description"],
          },
        },
      },
      required: ["tasks"],
    });

    // 构造提示
    let llmPrompt = `<prompt>${prompt}</prompt>`;
    llmPrompt += `\n<outputFormat>Return your output as JSON formatted according to the following schema: ${JSON.stringify(projectPlanSchema, null, 2)}</outputFormat>`
    for (const content of attachmentContents) {
      llmPrompt += `\n<attachment>${content}</attachment>`;
    }

    // 配置适当的提供者
    // Import and configure the appropriate provider
    const { google } = await import("@ai-sdk/google");
    const modelProvider = google("gemini-2.0-flash-lite");

    // let modelProvider;
    // switch (provider) {
    //   case "openai":
    //     const { openai } = await import("@ai-sdk/openai");
    //     modelProvider = openai(model);
    //     break;
    //   case "google":
    //     const { google } = await import("@ai-sdk/google");
    //     modelProvider = google(model);
    //     break;
    //   case "deepseek":
    //     const { deepseek } = await import("@ai-sdk/deepseek");
    //     modelProvider = deepseek(model);
    //     break;
    //   default:
    //     throw new AppError(`Invalid provider: ${provider}`, AppErrorCode.InvalidProvider);
    // }

    try {
      const { object } = await generateObject({
        model: modelProvider,
        schema: projectPlanSchema,
        prompt: llmPrompt,
      });
      
      // 使用BullMQTaskManager的createProject方法创建项目
      return await this.createProject(prompt, object.tasks, object.projectPlan);
    } catch (err: any) {
      console.log("err from generateProjectPlan", err);
      if (err.name === 'LoadAPIKeyError' || 
          err.message.includes('API key is missing') || 
          err.message.includes('You didn\'t provide an API key') ||
          err.message.includes('unregistered callers') ||
          (err.responseBody && err.responseBody.includes('Authentication Fails'))) {
        throw new AppError(
          `Missing API key environment variable required for ${provider}`,
          AppErrorCode.ConfigurationError,
          err
        );
      }
      
      if ((err.data?.error?.code === 'model_not_found') && 
          err.message.includes('model')) {
        throw new AppError(
          `Invalid model: ${model} is not available for ${provider}`,
          AppErrorCode.InvalidModel,
          err
        );
      }
      
      throw new AppError(
        "Failed to generate project plan due to an unexpected error",
        AppErrorCode.LLMGenerationError,
        err
      );
    }
  }

  /**
   * 读取附件文件
   * @param filename 文件名
   * @returns 文件内容
   */
  private async readAttachmentFile(filename: string): Promise<string> {
    try {
      const fs = await import('node:fs/promises');
      const filePath = path.resolve(process.cwd(), filename);
      return await fs.readFile(filePath, 'utf-8');
    } catch (error) {
      if (error instanceof Error && error.message.includes('ENOENT')) {
        throw new AppError(`Failed to read attachment file: ${filename} - File not found`, AppErrorCode.FileReadError, error);
      }
      throw new AppError(`Failed to read attachment file: ${filename}`, AppErrorCode.FileReadError, error);
    }
  }

  /**
   * 获取下一个任务
   * @param projectId 项目ID
   */
  public async getNextTask(projectId: string): Promise<OpenTaskSuccessData | { message: string }> {
    await this.ensureInitialized();
    
    try {
      const nextTask = await this.bullMQService.getNextTask(projectId);
      
      if (!nextTask) {
        return {
          message: `所有任务已完成并审批。等待项目完成审批。`
        };
      }
      
      return {
        projectId,
        task: this.convertToTask(nextTask)
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '获取下一个任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 审批任务完成
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async approveTaskCompletion(projectId: string, taskId: string): Promise<ApproveTaskSuccessData> {
    await this.ensureInitialized();
    
    try {
      const updatedTask = await this.bullMQService.approveTaskCompletion(projectId, taskId);
      
      return {
        projectId,
        task: {
          id: updatedTask.id,
          title: updatedTask.title,
          description: updatedTask.description,
          completedDetails: updatedTask.completedDetails,
          approved: updatedTask.approved
        }
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '审批任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 审批项目完成
   * @param projectId 项目ID
   */
  public async approveProjectCompletion(projectId: string): Promise<ApproveProjectSuccessData> {
    await this.ensureInitialized();
    
    try {
      const message = await this.bullMQService.approveProjectCompletion(projectId);
      
      return {
        projectId,
        message: "项目已完全完成并审批。"
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '审批项目完成失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 打开任务详情
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async openTaskDetails(projectId: string, taskId: string): Promise<OpenTaskSuccessData> {
    await this.ensureInitialized();
    
    try {
      const taskData = await this.bullMQService.getTask(projectId, taskId);
      
      return {
        projectId,
        task: this.convertToTask(taskData)
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '获取任务详情失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 列出项目
   * @param state 项目状态过滤器
   */
  public async listProjects(state?: TaskState): Promise<ListProjectsSuccessData> {
    await this.ensureInitialized();
    
    try {
      const projects = await this.bullMQService.listProjects(state);
      
      // 获取每个项目的任务统计信息
      const projectsWithStats = await Promise.all(
        projects.map(async project => {
          const tasks = await this.bullMQService.listTasks(project.projectId);
          
          const completedTasks = tasks.filter(t => t.status === "done").length;
          const approvedTasks = tasks.filter(t => t.approved).length;
          
          return {
            projectId: project.projectId,
            initialPrompt: project.initialPrompt,
            totalTasks: tasks.length,
            completedTasks,
            approvedTasks
          };
        })
      );
      
      return {
        message: `系统中当前的项目：`,
        projects: projectsWithStats
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '列出项目失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 列出任务
   * @param projectId 项目ID
   * @param state 任务状态过滤器
   */
  public async listTasks(projectId?: string, state?: TaskState): Promise<ListTasksSuccessData> {
    await this.ensureInitialized();
    
    try {
      const tasks = await this.bullMQService.listTasks(projectId, state);
      
      return {
        message: `系统中的任务${projectId ? ` (项目 ${projectId})` : ""}:\n找到 ${tasks.length} 个任务。`,
        tasks
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '列出任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 添加任务到项目
   * @param projectId 项目ID
   * @param tasks 任务列表
   */
  public async addTasksToProject(
    projectId: string,
    tasks: { title: string; description: string; toolRecommendations?: string; ruleRecommendations?: string }[]
  ): Promise<AddTasksSuccessData> {
    await this.ensureInitialized();
    
    try {
      const taskIds = await this.bullMQService.addTasksToProject(projectId, tasks);
      
      // 构建任务摘要
      const newTasks = tasks.map((task, index) => ({
        id: taskIds[index],
        title: task.title,
        description: task.description
      }));
      
      return {
        newTasks,
        message: `已向项目 ${projectId} 添加 ${newTasks.length} 个任务`
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '添加任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 更新任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @param updates 任务更新数据
   */
  public async updateTask(
    projectId: string,
    taskId: string,
    updates: {
      title?: string;
      description?: string;
      toolRecommendations?: string;
      ruleRecommendations?: string;
      status?: "not started" | "in progress" | "done";
      completedDetails?: string;
    }
  ): Promise<UpdateTaskSuccessData> {
    await this.ensureInitialized();
    
    try {
      const updatedTask = await this.bullMQService.updateTask(projectId, taskId, updates);
      
      // 获取项目数据以检查autoApprove设置
      const project = await this.bullMQService.readProject(projectId);
      
      // 生成消息（如果需要）
      let message: string | undefined = undefined;
      if (updates.status === 'done' && project.autoApprove === false) {
        message = `任务已标记为完成，但需要人工审批。\n要审批，用户应运行: npx taskqueue approve-task -- ${projectId} ${taskId}`;
      }
      
      return {
        task: this.convertToTask(updatedTask),
        message
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '更新任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 删除任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async deleteTask(projectId: string, taskId: string): Promise<DeleteTaskSuccessData> {
    await this.ensureInitialized();
    
    try {
      const message = await this.bullMQService.deleteTask(projectId, taskId);
      
      return {
        message: `任务 ${taskId} 已从项目 ${projectId} 中删除`
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '删除任务失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 读取项目
   * @param projectId 项目ID
   */
  public async readProject(projectId: string): Promise<ReadProjectSuccessData> {
    await this.ensureInitialized();
    
    try {
      const project = await this.bullMQService.readProject(projectId);
      
      return {
        projectId: project.projectId,
        initialPrompt: project.initialPrompt,
        projectPlan: project.projectPlan,
        completed: project.completed,
        autoApprove: project.autoApprove,
        tasks: project.tasks
      };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '读取项目失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 删除项目
   * @param projectId 项目ID 
   */
  public async deleteProject(projectId: string): Promise<{ status: string; message: string }> {
    await this.ensureInitialized();
    
    try {
      // BullMQService中需要实现deleteProject方法
      // 为简单起见，我们在这里通过抛出错误说明需要实现
      throw new AppError(
        '删除项目功能尚未在BullMQService中实现',
        AppErrorCode.Unknown
      );
      
      // 实现后应该是类似这样：
      // await this.bullMQService.deleteProject(projectId);
      // return {
      //   status: "project_deleted",
      //   message: `Project ${projectId} has been deleted.`
      // };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '删除项目失败',
        AppErrorCode.Unknown,
        error
      );
    }
  }

  /**
   * 将BullMQTaskData转换为Task接口
   * @param taskData BullMQ任务数据
   * @returns Task接口数据
   */
  private convertToTask(taskData: BullMQTaskData): Task {
    return {
      id: taskData.id,
      title: taskData.title,
      description: taskData.description,
      status: taskData.status,
      approved: taskData.approved,
      completedDetails: taskData.completedDetails,
      toolRecommendations: taskData.toolRecommendations,
      ruleRecommendations: taskData.ruleRecommendations
    };
  }
} 