// 从ioredis导入类型，但实例化时使用中间层
import type { Redis, RedisOptions } from 'ioredis';
// 删除旧的require导入
import { AppError, AppErrorCode } from '../types/errors.js';
// 移除多余的函数
import path from 'path';
import { fileURLToPath } from 'url';

/**
 * Redis连接管理器
 * 管理Redis连接的创建、获取和关闭
 */
export class RedisManager {
  private static instance: RedisManager;
  private connection: Redis | null = null;
  private connectionOptions: RedisOptions;
  private isInitialized = false;

  /**
   * 创建RedisManager实例
   * @param options Redis连接选项
   */
  private constructor(options?: RedisOptions) {
    this.connectionOptions = options || this.getDefaultOptions();
  }

  /**
   * 获取RedisManager单例实例
   * @param options Redis连接选项
   * @returns RedisManager实例
   */
  public static getInstance(options?: RedisOptions): RedisManager {
    if (!RedisManager.instance) {
      RedisManager.instance = new RedisManager(options);
    }
    return RedisManager.instance;
  }

  /**
   * 初始化Redis连接
   * @returns Redis连接是否成功初始化
   */
  public async initialize(): Promise<boolean> {
    if (this.isInitialized && this.connection) {
      return true;
    }

    try {
      // 使用CJS加载器导入Redis
      // 计算CJS文件的绝对路径
      const __dirname = path.dirname(fileURLToPath(import.meta.url));
      const redisLoaderPath = path.resolve(__dirname, 'redisLoader.cjs');
      
      // 动态导入CJS模块
      const RedisClient = (await import(redisLoaderPath)).default;
      
      // 创建Redis连接
      this.connection = new RedisClient({
        host: this.connectionOptions.host || 'localhost',
        port: this.connectionOptions.port || 6379,
        password: this.connectionOptions.password,
        db: this.connectionOptions.db || 0,
        retryStrategy: this.connectionOptions.retryStrategy,
        maxRetriesPerRequest: this.connectionOptions.maxRetriesPerRequest
      });
      
      // 设置错误处理器
      if (this.connection) {
        this.connection.on('error', (error: Error) => {
          console.error('Redis连接错误:', error);
        });

        // 测试连接
        await this.connection.ping();
        this.isInitialized = true;
        return true;
      }
      
      throw new AppError(
        'Redis连接初始化失败',
        AppErrorCode.RedisConnectionError
      );
    } catch (error) {
      console.error('Redis连接初始化失败:', error);
      this.isInitialized = false;
      throw new AppError(
        '无法连接到Redis服务器',
        AppErrorCode.RedisConnectionError,
        error
      );
    }
  }

  /**
   * 获取Redis连接
   * @returns Redis连接实例
   */
  public getConnection(): Redis {
    if (!this.isInitialized || !this.connection) {
      throw new AppError(
        'Redis连接未初始化',
        AppErrorCode.RedisConnectionError
      );
    }
    return this.connection;
  }

  /**
   * 检查Redis连接状态
   * @returns Redis连接是否就绪
   */
  public isReady(): boolean {
    return this.isInitialized && this.connection !== null;
  }

  /**
   * 关闭Redis连接
   */
  public async close(): Promise<void> {
    if (this.connection) {
      await this.connection.quit();
      this.connection = null;
      this.isInitialized = false;
    }
  }

  /**
   * 执行Redis命令并处理错误
   * @param callback Redis操作回调函数
   * @returns 命令执行结果
   */
  public async executeCommand<T>(callback: (redis: Redis) => Promise<T>): Promise<T> {
    if (!this.isInitialized || !this.connection) {
      await this.initialize();
    }

    try {
      return await callback(this.connection!);
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw new AppError(
        '执行Redis命令失败',
        AppErrorCode.RedisCommandError,
        error
      );
    }
  }

  /**
   * 获取默认的Redis连接选项
   * @returns 默认的Redis连接选项
   */
  private getDefaultOptions(): RedisOptions {
    return {
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379', 10),
      password: process.env.REDIS_PASSWORD,
      db: parseInt(process.env.REDIS_DB || '0', 10),
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      retryStrategy: (times: number) => {
        if (times > 10) {
          return null; // 停止重试
        }
        return Math.min(times * 100, 2000); // 指数退避策略
      }
    };
  }
} 