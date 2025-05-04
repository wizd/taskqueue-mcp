import { Task, Project, TaskState } from "../types/data.js";
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

/**
 * 任务管理器基类
 * 定义所有任务管理器实现必须提供的接口
 */
export abstract class TaskManagerBase {
  
  /**
   * 确保管理器已初始化
   */
  protected abstract ensureInitialized(): Promise<void>;

  /**
   * 创建新项目
   * @param initialPrompt 初始提示
   * @param tasks 任务列表
   * @param projectPlan 项目计划
   * @param autoApprove 是否自动审批
   */
  public abstract createProject(
    initialPrompt: string,
    tasks: { title: string; description: string; toolRecommendations?: string; ruleRecommendations?: string }[],
    projectPlan?: string,
    autoApprove?: boolean
  ): Promise<ProjectCreationSuccessData>;

  /**
   * 生成项目计划
   * @param options 参数选项
   */
  public abstract generateProjectPlan(options: {
    prompt: string;
    provider: string;
    model: string;
    attachments: string[];
  }): Promise<ProjectCreationSuccessData>;

  /**
   * 获取下一个任务
   * @param projectId 项目ID
   */
  public abstract getNextTask(projectId: string): Promise<OpenTaskSuccessData | { message: string }>;

  /**
   * 审批任务完成
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public abstract approveTaskCompletion(projectId: string, taskId: string): Promise<ApproveTaskSuccessData>;

  /**
   * 审批项目完成
   * @param projectId 项目ID
   */
  public abstract approveProjectCompletion(projectId: string): Promise<ApproveProjectSuccessData>;

  /**
   * 打开任务详情
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public abstract openTaskDetails(projectId: string, taskId: string): Promise<OpenTaskSuccessData>;

  /**
   * 列出项目
   * @param state 项目状态过滤器
   */
  public abstract listProjects(state?: TaskState): Promise<ListProjectsSuccessData>;

  /**
   * 列出任务
   * @param projectId 项目ID
   * @param state 任务状态过滤器
   */
  public abstract listTasks(projectId?: string, state?: TaskState): Promise<ListTasksSuccessData>;

  /**
   * 添加任务到项目
   * @param projectId 项目ID
   * @param tasks 任务列表
   */
  public abstract addTasksToProject(
    projectId: string,
    tasks: { title: string; description: string; toolRecommendations?: string; ruleRecommendations?: string }[]
  ): Promise<AddTasksSuccessData>;

  /**
   * 更新任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   * @param updates 任务更新数据
   */
  public abstract updateTask(
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
  ): Promise<UpdateTaskSuccessData>;

  /**
   * 删除任务
   * @param projectId 项目ID
   * @param taskId 任务ID
   */
  public abstract deleteTask(projectId: string, taskId: string): Promise<DeleteTaskSuccessData>;

  /**
   * 读取项目
   * @param projectId 项目ID
   */
  public abstract readProject(projectId: string): Promise<ReadProjectSuccessData>;

  /**
   * 从磁盘重新加载数据
   */
  public abstract reloadFromDisk(): Promise<void>;
} 