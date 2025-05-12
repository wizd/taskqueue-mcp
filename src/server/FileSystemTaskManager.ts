import * as path from "node:path";
import {
  Task,
  TaskManagerFile,
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
import { FileSystemService } from "./FileSystemService.js";
import { generateObject, jsonSchema } from "@wizdy/ai";
import { TaskManagerBase } from "./TaskManagerBase.js";
import { modelProvider } from "../../lib/ai/provider.js";

// Default path follows platform-specific conventions
const DEFAULT_PATH = path.join(FileSystemService.getAppDataDir(), "tasks.json");
const TASK_FILE_PATH = process.env.TASK_MANAGER_FILE_PATH || DEFAULT_PATH;

interface ProjectPlanOutput {
  projectPlan: string;
  tasks: Array<{
    title: string;
    description: string;
    toolRecommendations?: string;
    ruleRecommendations?: string;
  }>;
}

/**
 * 基于文件系统的任务管理器
 * 继承自TaskManagerBase，实现通过JSON文件存储任务数据
 */
export class FileSystemTaskManager extends TaskManagerBase {
  private projectCounter = 0;
  private taskCounter = 0;
  private data: TaskManagerFile = { projects: [] };
  private fileSystemService: FileSystemService;
  private initialized: Promise<void>;

  /**
   * 创建FileSystemTaskManager实例
   * @param filePath 任务文件路径
   */
  constructor(filePath?: string) {
    super();
    this.fileSystemService = new FileSystemService(filePath || TASK_FILE_PATH);
    
    // 检查当前存储模式，如果是仅BullMQ模式，则不需要加载文件
    const storageMode = process.env.TASKQUEUE_STORAGE_MODE;
    if (storageMode && (['bullmq_only', 'bullmq'] as string[]).includes(storageMode.toLowerCase())) {
      // 在BullMQ模式下不需要初始化文件
      this.initialized = Promise.resolve();
    } else {
      // 正常初始化
      this.initialized = this.loadTasks();
    }
  }

  /**
   * 加载任务
   */
  private async loadTasks(): Promise<void> {
    try {
      const { data, maxProjectId, maxTaskId } = await this.fileSystemService.loadAndInitializeTasks();
      this.data = data;
      this.projectCounter = maxProjectId;
      this.taskCounter = maxTaskId;
    } catch (error) {
      throw new AppError(
        "Failed to load tasks from disk",
        AppErrorCode.FileReadError,
        error
      );
    }
  }

  /**
   * 确保管理器已初始化
   */
  protected async ensureInitialized() {
    try {
      await this.initialized;
    } catch (error) {
      // If initialization failed, throw an AppError that can be handled by the tool executor
      throw new AppError(
        'Failed to initialize task manager',
        AppErrorCode.FileReadError,
        error
      );
    }
  }

  /**
   * 从磁盘重新加载数据
   */
  public async reloadFromDisk(): Promise<void> {
    try {
      const data = await this.fileSystemService.reloadTasks();
      this.data = data;
      const { maxProjectId, maxTaskId } = this.fileSystemService.calculateMaxIds(data);
      this.projectCounter = maxProjectId;
      this.taskCounter = maxTaskId;
    } catch (error) {
      // Propagate as AppError to be handled by the tool executor
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        'Failed to reload tasks from disk',
        AppErrorCode.FileReadError,
        error
      );
    }
  }

  /**
   * 保存任务到文件
   */
  private async saveTasks() {
    await this.fileSystemService.saveTasks(this.data);
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
    await this.reloadFromDisk();
    
    this.projectCounter += 1;
    const projectId = `proj-${this.projectCounter}`;

    const newTasks: Task[] = [];
    for (const taskDef of tasks) {
      this.taskCounter += 1;
      newTasks.push({
        id: `task-${this.taskCounter}`,
        title: taskDef.title,
        description: taskDef.description,
        status: "not started",
        approved: false,
        completedDetails: "",
        toolRecommendations: taskDef.toolRecommendations,
        ruleRecommendations: taskDef.ruleRecommendations,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });
    }

    const newProject: Project = {
      projectId,
      initialPrompt,
      projectPlan: projectPlan || initialPrompt,
      tasks: newTasks,
      completed: false,
      autoApprove: autoApprove === false ? false : true,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    this.data.projects.push(newProject);
    await this.saveTasks();

    return {
      projectId,
      totalTasks: newTasks.length,
      tasks: newTasks.map((t) => ({
        id: t.id,
        title: t.title,
        description: t.description,
      })),
      message: `Project ${projectId} created with ${newTasks.length} tasks.`,
    };
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

    // Read all attachment files
    const attachmentContents: string[] = [];
    for (const filename of attachments) {
      try {
        const content = await this.fileSystemService.readAttachmentFile(filename);
        attachmentContents.push(content);
      } catch (error) {
        throw new AppError(`Failed to read attachment file: ${filename}`, AppErrorCode.FileReadError, error);
      }
    }

    // Define the schema for the LLM's response using jsonSchema helper
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

    // Wrap prompt and attachments in XML tags
    let llmPrompt = `<prompt>${prompt}</prompt>`;
    llmPrompt += `\n<outputFormat>Return your output as JSON formatted according to the following schema: ${JSON.stringify(projectPlanSchema, null, 2)}</outputFormat>`
    for (const content of attachmentContents) {
      llmPrompt += `\n<attachment>${content}</attachment>`;
    }

    try {
      const { object } = await generateObject({
        model: modelProvider,
        schema: projectPlanSchema,
        prompt: llmPrompt,
      });
      return await this.createProject(prompt, object.tasks, object.projectPlan);
    } catch (err: any) {
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
      // Check for invalid model errors by looking at the error code, type, and message
      if ((err.data?.error?.code === 'model_not_found') && 
          err.message.includes('model')) {
        throw new AppError(
          `Invalid model: ${model} is not available for ${provider}`,
          AppErrorCode.InvalidModel,
          err
        );
      }
      // For unknown errors, preserve the original error but wrap it
      throw new AppError(
        "Failed to generate project plan due to an unexpected error",
        AppErrorCode.LLMGenerationError,
        err
      );
    }
  }

  /**
   * 获取下一个任务
   * @param projectId 项目ID
   */
  public async getNextTask(projectId: string): Promise<OpenTaskSuccessData | { message: string }> {
    await this.ensureInitialized();
    await this.reloadFromDisk();
    
    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }
    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    if (!proj.tasks.length) {
      throw new AppError('Project has no tasks', AppErrorCode.TaskNotFound);
    }

    const nextTask = proj.tasks.find((t) => !(t.status === "done" && t.approved));
    if (!nextTask) {
      // all tasks done and approved?
      const allDoneAndApproved = proj.tasks.every((t) => t.status === "done" && t.approved);
      if (allDoneAndApproved && !proj.completed) {
        return {
          message: `All tasks have been completed and approved. Awaiting project completion approval.`
        };
      }
      throw new AppError('No incomplete or unapproved tasks found', AppErrorCode.TaskNotFound);
    }

    return {
      projectId: proj.projectId,
      task: { ...nextTask },
    };
  }

  /**
   * 审批任务完成
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async approveTaskCompletion(projectId: string, taskId: string): Promise<ApproveTaskSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    const task = proj.tasks.find((t) => t.id === taskId);
    if (!task) {
      throw new AppError(`Task ${taskId} not found`, AppErrorCode.TaskNotFound);
    }

    if (task.status !== "done") {
      throw new AppError('Task is not done yet', AppErrorCode.TaskNotDone);
    }

    if (task.approved) {
      throw new AppError('Task is already approved', AppErrorCode.TaskAlreadyApproved);
    }

    task.approved = true;
    
    // 更新时间戳
    task.updatedAt = new Date().toISOString();
    proj.updatedAt = new Date().toISOString();

    await this.saveTasks();
    return {
      projectId: proj.projectId,
      task: {
        id: task.id,
        title: task.title,
        description: task.description,
        completedDetails: task.completedDetails,
        approved: task.approved,
      },
    };
  }

  /**
   * 审批项目完成
   * @param projectId 项目ID
   */
  public async approveProjectCompletion(projectId: string): Promise<ApproveProjectSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    const allDone = proj.tasks.every((t) => t.status === "done");
    if (!allDone) {
      throw new AppError('Not all tasks are done', AppErrorCode.TasksNotAllDone);
    }

    const allApproved = proj.tasks.every((t) => t.status === "done" && t.approved);
    if (!allApproved) {
      throw new AppError('Not all done tasks are approved', AppErrorCode.TasksNotAllApproved);
    }

    proj.completed = true;
    
    // 更新项目的更新时间
    proj.updatedAt = new Date().toISOString();
    
    await this.saveTasks();

    return {
      projectId: proj.projectId,
      message: "Project is fully completed and approved.",
    };
  }

  /**
   * 打开任务详情
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async openTaskDetails(projectId: string, taskId: string): Promise<OpenTaskSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    const project = this.data.projects.find((p) => p.projectId === projectId);
    if (!project) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    const target = project.tasks.find((t) => t.id === taskId);
    if (!target) {
      throw new AppError(`Task ${taskId} not found`, AppErrorCode.TaskNotFound);
    }

    return {
      projectId: project.projectId,
      task: { ...target },
    };
  }

  /**
   * 列出项目
   * @param state 项目状态过滤器
   */
  public async listProjects(state?: TaskState): Promise<ListProjectsSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    if (state && !["all", "open", "completed", "pending_approval"].includes(state)) {
      throw new AppError(`Invalid state filter: ${state}`, AppErrorCode.InvalidState);
    }

    let filteredProjects = [...this.data.projects];

    if (state && state !== "all") {
      filteredProjects = filteredProjects.filter((p) => {
        switch (state) {
          case "open":
            return !p.completed;
          case "completed":
            return p.completed;
          case "pending_approval":
            return !p.completed && p.tasks.every((t) => t.status === "done");
          default:
            return true;
        }
      });
    }

    return {
      message: `Current projects in the system:`,
      projects: filteredProjects.map((p) => ({
        projectId: p.projectId,
        initialPrompt: p.initialPrompt,
        totalTasks: p.tasks.length,
        completedTasks: p.tasks.filter((t) => t.status === "done").length,
        approvedTasks: p.tasks.filter((t) => t.approved).length,
        createdAt: p.createdAt || new Date().toISOString(),
        updatedAt: p.updatedAt || new Date().toISOString(),
      })),
    };
  }

  /**
   * 列出任务
   * @param projectId 项目ID
   * @param state 任务状态过滤器
   */
  public async listTasks(projectId?: string, state?: TaskState): Promise<ListTasksSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    if (state && !["all", "open", "completed", "pending_approval"].includes(state)) {
      throw new AppError(`Invalid state filter: ${state}`, AppErrorCode.InvalidState);
    }

    let allTasks: Task[] = [];

    if (projectId) {
      const proj = this.data.projects.find((p) => p.projectId === projectId);
      if (!proj) {
        throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
      }
      allTasks = [...proj.tasks];
    } else {
      // Collect tasks from all projects
      allTasks = this.data.projects.flatMap((p) => p.tasks);
    }

    if (state && state !== "all") {
      allTasks = allTasks.filter((task) => {
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

    return {
      message: `Tasks in the system${projectId ? ` for project ${projectId}` : ""}:\n${allTasks.length} tasks found.`,
      tasks: allTasks,
    };
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
    await this.reloadFromDisk();

    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    const newTasks: Task[] = [];
    const now = new Date().toISOString();
    for (const taskDef of tasks) {
      this.taskCounter += 1;
      const newTask: Task = {
        id: `task-${this.taskCounter}`,
        title: taskDef.title,
        description: taskDef.description,
        status: "not started",
        approved: false,
        completedDetails: "",
        toolRecommendations: taskDef.toolRecommendations,
        ruleRecommendations: taskDef.ruleRecommendations,
        createdAt: now,
        updatedAt: now,
      };
      newTasks.push(newTask);
      proj.tasks.push(newTask);
    }
    
    // 更新项目的更新时间
    proj.updatedAt = now;

    await this.saveTasks();

    return {
      newTasks: newTasks.map((t) => ({
        id: t.id,
        title: t.title,
        description: t.description,
      })),
      message: `Added ${newTasks.length} tasks to project ${projectId}`,
    };
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
    await this.reloadFromDisk();

    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    const task = proj.tasks.find((t) => t.id === taskId);
    if (!task) {
      throw new AppError(`Task ${taskId} not found`, AppErrorCode.TaskNotFound);
    }

    if (task.approved) {
      throw new AppError('Cannot modify an approved task', AppErrorCode.CannotModifyApprovedTask);
    }

    // Apply updates
    Object.assign(task, updates);
    
    // 更新时间戳
    task.updatedAt = new Date().toISOString();
    proj.updatedAt = new Date().toISOString();

    // Generate message if needed
    let message: string | undefined = undefined;
    if (updates.status === 'done' && proj.autoApprove === false) {
      message = `Task marked as done but requires human approval.\nTo approve, user should run: npx taskqueue approve-task -- ${projectId} ${taskId}`;
    }

    await this.saveTasks();
    return { task, message };
  }

  /**
   * 删除任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public async deleteTask(projectId: string, taskId: string): Promise<DeleteTaskSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    const proj = this.data.projects.find((p) => p.projectId === projectId);
    if (!proj) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    if (proj.completed) {
      throw new AppError('Project is already completed', AppErrorCode.ProjectAlreadyCompleted);
    }

    const taskIndex = proj.tasks.findIndex((t) => t.id === taskId);
    if (taskIndex === -1) {
      throw new AppError(`Task ${taskId} not found`, AppErrorCode.TaskNotFound);
    }

    const task = proj.tasks[taskIndex];
    if (task.approved) {
      throw new AppError('Cannot delete an approved task', AppErrorCode.CannotModifyApprovedTask);
    }

    proj.tasks.splice(taskIndex, 1);
    await this.saveTasks();

    return {
      message: `Task ${taskId} deleted from project ${projectId}`,
    };
  }

  /**
   * 读取项目
   * @param projectId 项目ID
   */
  public async readProject(projectId: string): Promise<ReadProjectSuccessData> {
    await this.ensureInitialized();
    await this.reloadFromDisk();

    const project = this.data.projects.find((p) => p.projectId === projectId);
    if (!project) {
      throw new AppError(`Project ${projectId} not found`, AppErrorCode.ProjectNotFound);
    }

    // 确保任务有时间戳
    project.tasks.forEach(task => {
      if (!task.createdAt) task.createdAt = new Date().toISOString();
      if (!task.updatedAt) task.updatedAt = new Date().toISOString();
    });
    
    // 确保项目有时间戳
    if (!project.createdAt) project.createdAt = new Date().toISOString();
    if (!project.updatedAt) project.updatedAt = new Date().toISOString();

    return {
      projectId: project.projectId,
      initialPrompt: project.initialPrompt,
      projectPlan: project.projectPlan,
      completed: project.completed,
      autoApprove: project.autoApprove,
      tasks: project.tasks,
      createdAt: project.createdAt,
      updatedAt: project.updatedAt,
    };
  }

  /**
   * 删除项目
   * @param projectId 项目ID
   */
  public async deleteProject(projectId: string): Promise<{ status: string; message: string }> {
    await this.ensureInitialized();
    await this.reloadFromDisk();
    
    const projectIndex = this.data.projects.findIndex(p => p.projectId === projectId);
    if (projectIndex === -1) {
      throw new AppError(
        `Project not found: ${projectId}`,
        AppErrorCode.ProjectNotFound
      );
    }
    
    this.data.projects.splice(projectIndex, 1);
    await this.saveTasks();
    
    return {
      status: "project_deleted",
      message: `Project ${projectId} has been deleted.`
    };
  }
} 