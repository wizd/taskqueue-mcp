import { TaskManagerBase } from './TaskManagerBase.js';
import { FileSystemTaskManager } from './FileSystemTaskManager.js';
import { BullMQTaskManager } from './BullMQTaskManager.js';
import { MigrationMode, BullMQServiceOptions } from '../types/bullmq.js';

/**
 * 任务管理器工厂类
 * 用于创建适当的任务管理器实例
 */
export class TaskManagerFactory {
  /**
   * 创建任务管理器实例
   * @param mode 存储模式
   * @param options 配置选项
   * @returns 任务管理器实例
   */
  public static createTaskManager(
    mode?: MigrationMode, 
    options?: {
      filePath?: string;
      bullmqOptions?: BullMQServiceOptions;
    }
  ): TaskManagerBase {
    // 如果未指定模式，从环境变量获取
    const storageMode = mode || this.getStorageModeFromEnv();
    
    switch (storageMode) {
      case MigrationMode.FILE_ONLY:
        return new FileSystemTaskManager(options?.filePath);
      
      case MigrationMode.BULLMQ_ONLY:
        return new BullMQTaskManager(options?.bullmqOptions);
      
      case MigrationMode.DUAL_WRITE:
      case MigrationMode.READ_BULLMQ_WRITE_BOTH:
        // 在这些模式下，我们需要实现混合写入/读取的管理器
        // 这部分可以在后续阶段实现，目前只是预留
        console.warn(`存储模式 ${storageMode} 尚未完全实现，暂时使用BullMQ模式`);
        return new BullMQTaskManager(options?.bullmqOptions);
      
      default:
        // 默认使用文件系统存储
        return new FileSystemTaskManager(options?.filePath);
    }
  }

  /**
   * 从环境变量获取存储模式
   * @returns 解析的存储模式
   */
  private static getStorageModeFromEnv(): MigrationMode {
    const modeStr = process.env.TASKQUEUE_STORAGE_MODE;
    
    if (!modeStr) {
      return MigrationMode.FILE_ONLY; // 默认模式
    }
    
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
        console.warn(`未知的存储模式: ${modeStr}，使用默认的文件存储模式`);
        return MigrationMode.FILE_ONLY;
    }
  }
} 