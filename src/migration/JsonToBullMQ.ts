import { TaskManagerFile } from '../types/data.js';
import { FileSystemService } from '../server/FileSystemService.js';
import { BullMQService } from '../server/BullMQService.js';
import { AppError, AppErrorCode } from '../types/errors.js';
import { MigrationMode } from '../types/bullmq.js';

/**
 * JSON到BullMQ数据迁移工具
 * 用于将现有JSON文件数据迁移到BullMQ/Redis
 */
export class JsonToBullMQ {
  private fileSystemService: FileSystemService;
  private bullMQService: BullMQService;

  /**
   * 创建迁移工具实例
   * @param filePath JSON文件路径
   * @param bullMQOptions BullMQ服务配置
   */
  constructor(filePath: string, bullMQOptions = {}) {
    this.fileSystemService = new FileSystemService(filePath);
    this.bullMQService = new BullMQService(bullMQOptions);
  }

  /**
   * 执行迁移
   * @returns 迁移结果统计
   */
  public async migrate(): Promise<{
    projects: number;
    tasks: number;
    errors: { projectId: string; taskId?: string; error: string }[];
  }> {
    try {
      // 从文件加载数据
      const { data } = await this.fileSystemService.loadAndInitializeTasks();
      
      const stats = {
        projects: 0,
        tasks: 0,
        errors: [] as { projectId: string; taskId?: string; error: string }[]
      };

      // 按项目迁移
      for (const project of data.projects) {
        try {
          // 创建项目
          const projectId = await this.bullMQService.createProject(
            project.initialPrompt,
            project.projectPlan,
            project.autoApprove
          );

          // 添加项目的任务
          if (project.tasks && project.tasks.length > 0) {
            const taskDefs = project.tasks.map(task => ({
              title: task.title,
              description: task.description,
              toolRecommendations: task.toolRecommendations,
              ruleRecommendations: task.ruleRecommendations
            }));

            const taskIds = await this.bullMQService.addTasksToProject(projectId, taskDefs);

            // 更新任务状态和审批
            for (let i = 0; i < project.tasks.length; i++) {
              const task = project.tasks[i];
              const taskId = taskIds[i];

              // 更新任务状态
              if (task.status !== 'not started') {
                await this.bullMQService.updateTask(projectId, taskId, {
                  status: task.status,
                  completedDetails: task.completedDetails
                });
              }

              // 审批任务
              if (task.status === 'done' && task.approved) {
                try {
                  await this.bullMQService.approveTaskCompletion(projectId, taskId);
                } catch (error) {
                  stats.errors.push({
                    projectId,
                    taskId,
                    error: `审批任务失败: ${error instanceof Error ? error.message : String(error)}`
                  });
                }
              }
            }

            stats.tasks += taskIds.length;
          }

          // 如果项目已完成，标记为完成
          if (project.completed) {
            try {
              await this.bullMQService.approveProjectCompletion(projectId);
            } catch (error) {
              stats.errors.push({
                projectId,
                error: `项目完成审批失败: ${error instanceof Error ? error.message : String(error)}`
              });
            }
          }

          stats.projects++;
        } catch (error) {
          stats.errors.push({
            projectId: project.projectId,
            error: `迁移项目失败: ${error instanceof Error ? error.message : String(error)}`
          });
        }
      }

      return stats;
    } catch (error) {
      throw new AppError(
        '数据迁移失败',
        AppErrorCode.MigrationError,
        error
      );
    }
  }

  /**
   * 验证迁移结果
   * 比较JSON数据和BullMQ数据是否一致
   * @returns 验证结果
   */
  public async validateMigration(): Promise<{
    success: boolean;
    mismatches: {
      type: 'project' | 'task';
      id: string;
      field: string;
      jsonValue: any;
      bullmqValue: any;
    }[];
  }> {
    try {
      // 从文件加载数据
      const { data } = await this.fileSystemService.loadAndInitializeTasks();
      
      const result = {
        success: true,
        mismatches: [] as {
          type: 'project' | 'task';
          id: string;
          field: string;
          jsonValue: any;
          bullmqValue: any;
        }[]
      };

      // 获取所有项目
      const bullmqProjects = await this.bullMQService.listProjects();
      
      // 比较项目数量
      if (data.projects.length !== bullmqProjects.length) {
        result.mismatches.push({
          type: 'project',
          id: 'all',
          field: 'count',
          jsonValue: data.projects.length,
          bullmqValue: bullmqProjects.length
        });
        result.success = false;
      }

      // 按项目比较
      for (const jsonProject of data.projects) {
        const bullmqProject = bullmqProjects.find(p => p.projectId === jsonProject.projectId);
        
        if (!bullmqProject) {
          result.mismatches.push({
            type: 'project',
            id: jsonProject.projectId,
            field: 'missing',
            jsonValue: true,
            bullmqValue: false
          });
          result.success = false;
          continue;
        }

        // 比较项目字段
        this.compareFields(
          'project',
          jsonProject.projectId,
          jsonProject,
          bullmqProject,
          ['initialPrompt', 'projectPlan', 'completed', 'autoApprove'],
          result.mismatches
        );

        // 获取项目的任务
        const bullmqTasks = await this.bullMQService.listTasks(jsonProject.projectId);
        
        // 比较任务数量
        if (jsonProject.tasks.length !== bullmqTasks.length) {
          result.mismatches.push({
            type: 'project',
            id: jsonProject.projectId,
            field: 'taskCount',
            jsonValue: jsonProject.tasks.length,
            bullmqValue: bullmqTasks.length
          });
          result.success = false;
        }

        // 按任务比较
        for (const jsonTask of jsonProject.tasks) {
          const bullmqTask = bullmqTasks.find(t => t.id === jsonTask.id);
          
          if (!bullmqTask) {
            result.mismatches.push({
              type: 'task',
              id: jsonTask.id,
              field: 'missing',
              jsonValue: true,
              bullmqValue: false
            });
            result.success = false;
            continue;
          }

          // 比较任务字段
          this.compareFields(
            'task',
            jsonTask.id,
            jsonTask,
            bullmqTask,
            ['title', 'description', 'status', 'approved', 'completedDetails'],
            result.mismatches
          );
        }
      }

      return result;
    } catch (error) {
      throw new AppError(
        '迁移验证失败',
        AppErrorCode.MigrationError,
        error
      );
    }
  }

  /**
   * 比较两个对象的指定字段
   * @param type 对象类型
   * @param id 对象ID
   * @param jsonObj JSON数据对象
   * @param bullmqObj BullMQ数据对象
   * @param fields 要比较的字段
   * @param mismatches 不匹配记录数组
   */
  private compareFields(
    type: 'project' | 'task',
    id: string,
    jsonObj: any,
    bullmqObj: any,
    fields: string[],
    mismatches: {
      type: 'project' | 'task';
      id: string;
      field: string;
      jsonValue: any;
      bullmqValue: any;
    }[]
  ): void {
    for (const field of fields) {
      if (JSON.stringify(jsonObj[field]) !== JSON.stringify(bullmqObj[field])) {
        mismatches.push({
          type,
          id,
          field,
          jsonValue: jsonObj[field],
          bullmqValue: bullmqObj[field]
        });
      }
    }
  }

  /**
   * 获取当前存储模式
   * @returns 当前配置的存储模式
   */
  public static getCurrentMode(): MigrationMode {
    const mode = process.env.TASKQUEUE_STORAGE_MODE;
    
    switch (mode) {
      case 'file_only':
        return MigrationMode.FILE_ONLY;
      case 'bullmq_only':
        return MigrationMode.BULLMQ_ONLY;
      case 'dual_write':
        return MigrationMode.DUAL_WRITE;
      case 'read_bullmq_write_both':
        return MigrationMode.READ_BULLMQ_WRITE_BOTH;
      default:
        return MigrationMode.FILE_ONLY; // 默认使用文件存储
    }
  }

  /**
   * 解析存储模式字符串
   * @param modeStr 存储模式字符串
   * @returns 存储模式枚举值
   */
  public static parseMode(modeStr: string): MigrationMode {
    switch (modeStr.toLowerCase()) {
      case 'file_only':
      case 'file':
        return MigrationMode.FILE_ONLY;
      case 'bullmq_only':
      case 'bullmq':
        return MigrationMode.BULLMQ_ONLY;
      case 'dual_write':
      case 'dual':
        return MigrationMode.DUAL_WRITE;
      case 'read_bullmq_write_both':
      case 'read_bullmq':
        return MigrationMode.READ_BULLMQ_WRITE_BOTH;
      default:
        throw new AppError(
          `无效的存储模式: ${modeStr}`,
          AppErrorCode.InvalidArgument
        );
    }
  }
} 