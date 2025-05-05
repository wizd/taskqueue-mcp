import { FileSystemTaskManager } from './FileSystemTaskManager.js';
import { BullMQTaskManager } from './BullMQTaskManager.js';
import { TaskManagerBase } from './TaskManagerBase.js';
import { MigrationMode } from '../types/bullmq.js';
import dotenv from 'dotenv';

// 加载环境变量
dotenv.config();

/**
 * TaskManager工厂类
 * 负责创建和配置TaskManager实例
 */
export class TaskManagerFactory {
  // 单例TaskManager实例
  private static instance: TaskManagerBase;

  /**
   * 获取或创建TaskManager实例
   * @param mode 迁移模式
   * @returns TaskManager实例
   */
  public static createTaskManager(mode?: MigrationMode): TaskManagerBase {
    // 如果没有指定模式，从环境变量获取
    if (!mode) {
      const envMode = process.env.MIGRATION_MODE;
      if (envMode && Object.values(MigrationMode).includes(envMode as MigrationMode)) {
        mode = envMode as MigrationMode;
      } else {
        mode = MigrationMode.FILE_ONLY; // 默认模式
      }
    }

    if (!this.instance) {
      switch (mode) {
        case MigrationMode.BULLMQ_ONLY:
          this.instance = new BullMQTaskManager({
            connection: {
              host: process.env.REDIS_HOST || 'localhost',
              port: parseInt(process.env.REDIS_PORT || '6379', 10),
              password: process.env.REDIS_PASSWORD
            },
            defaultJobOptions: {
              attempts: 3,
              removeOnComplete: true
            }
          });
          break;
        case MigrationMode.DUAL_WRITE:
        case MigrationMode.READ_BULLMQ_WRITE_BOTH:
          // 这里应该创建一个DualWriteTaskManager，但目前暂未实现
          // 回退到文件系统模式
          this.instance = new FileSystemTaskManager();
          break;
        case MigrationMode.FILE_ONLY:
        default:
          this.instance = new FileSystemTaskManager();
          break;
      }
    }

    return this.instance;
  }
  
  /**
   * 获取TaskManager实例，并设置租户ID
   * @param mode 迁移模式
   * @param tenantId 租户ID
   * @returns TaskManager实例
   */
  public static createTaskManagerWithTenant(mode?: MigrationMode, tenantId?: string): TaskManagerBase {
    const taskManager = this.createTaskManager(mode);
    
    // 如果是BullMQTaskManager并且指定了租户ID，设置租户ID
    if (taskManager instanceof BullMQTaskManager && tenantId) {
      taskManager.setTenantId(tenantId);
    }
    
    return taskManager;
  }
} 