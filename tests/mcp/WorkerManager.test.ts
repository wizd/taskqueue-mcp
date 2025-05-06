import { jest } from '@jest/globals';
import { WorkerManager } from '../../src/server/WorkerManager.js';
import { RedisManager } from '../../src/server/RedisManager.js';
import { Worker, Job, Queue } from 'bullmq';
import { BullMQTaskData } from '../../src/types/bullmq.js';

// 定义模拟类型，解决类型错误
type MockedFunction<T extends (...args: any) => any> = jest.MockedFunction<T>;

// 模拟依赖
jest.mock('../../src/server/RedisManager.js');
jest.mock('bullmq');
jest.mock('../../src/server/Logger.js', () => {
  return {
    Logger: jest.fn().mockImplementation(() => ({
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn()
    }))
  };
});

describe('WorkerManager', () => {
  let workerManager: WorkerManager;
  let mockRedisConnection: any;
  let mockRedisManager: any;
  
  beforeEach(() => {
    // 清理模拟
    jest.clearAllMocks();
    
    // 模拟Redis连接
    mockRedisConnection = {
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      keys: jest.fn().mockResolvedValue(['tenant:test:project:proj-1:metadata']),
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      hgetall: jest.fn().mockResolvedValue({ projectId: 'proj-1' })
    };
    
    // 模拟RedisManager
    mockRedisManager = {
      getInstance: jest.fn().mockReturnThis(),
      getConnection: jest.fn().mockReturnValue(mockRedisConnection),
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      initialize: jest.fn().mockResolvedValue(undefined)
    };
    
    // 应用到RedisManager模块
    (RedisManager.getInstance as jest.Mock).mockImplementation(() => mockRedisManager);
    (RedisManager.prototype.getConnection as jest.Mock) = jest.fn().mockReturnValue(mockRedisConnection);
    
    // 模拟Worker构造函数
    (Worker as unknown as jest.Mock).mockImplementation(() => {
      return {
        on: jest.fn(),
        // @ts-ignore - 忽略Jest模拟函数的返回类型错误
        close: jest.fn().mockResolvedValue(undefined)
      };
    });
    
    // 创建WorkerManager实例
    workerManager = new WorkerManager();
  });
  
  test('初始化时应创建Worker实例', async () => {
    // 调用initAll方法
    await workerManager.initAll();
    
    // 验证是否查询项目元数据键
    expect(mockRedisConnection.keys).toHaveBeenCalled();
    
    // 验证是否创建了Worker实例
    expect(Worker).toHaveBeenCalled();
  });
  
  test('register应为项目创建新Worker', async () => {
    // 调用register方法
    await workerManager.register('proj-1');
    
    // 验证是否创建了Worker实例
    expect(Worker).toHaveBeenCalled();
    
    // 再次调用不应创建新Worker
    jest.clearAllMocks();
    await workerManager.register('proj-1');
    expect(Worker).not.toHaveBeenCalled();
  });
  
  test('unregister应关闭并移除Worker', async () => {
    // 首先注册Worker
    const worker = await workerManager.register('proj-1');
    
    // 然后注销它
    await workerManager.unregister('proj-1');
    
    // 验证是否调用了close方法
    expect(worker.close).toHaveBeenCalled();
  });
  
  test('shutdownAll应关闭所有Worker', async () => {
    // 注册多个Worker
    const worker1 = await workerManager.register('proj-1');
    const worker2 = await workerManager.register('proj-2');
    
    // 关闭所有Worker
    await workerManager.shutdownAll();
    
    // 验证所有Worker都调用了close方法
    expect(worker1.close).toHaveBeenCalled();
    expect(worker2.close).toHaveBeenCalled();
  });
  
  test('setPrefix应更新前缀并触发Worker重新初始化', async () => {
    // 模拟shutdownAll和initAll方法
    const shutdownSpy = jest.spyOn(workerManager, 'shutdownAll').mockResolvedValue();
    const initSpy = jest.spyOn(workerManager, 'initAll').mockResolvedValue();
    
    // 注册Worker以确保workers映射非空
    await workerManager.register('proj-1');
    
    // 设置新前缀
    workerManager.setPrefix('tenant:test2:');
    
    // 验证是否调用了shutdownAll
    expect(shutdownSpy).toHaveBeenCalled();
    
    // 测试initAll是否作为Promise链的一部分被调用
    // 由于这是异步的，需要等待Promise调用栈清空
    await Promise.resolve();
    expect(initSpy).toHaveBeenCalled();
  });
  
  test('processorFn应正确处理任务并更新状态', async () => {
    // 创建模拟Job对象
    const mockJob = {
      id: 'task-1',
      data: {
        id: 'task-1',
        title: '测试任务',
        description: '测试描述',
        status: 'not started' as const,
        approved: false,
        completedDetails: '',
        projectId: 'proj-1',
        createdAt: Date.now(),
        updatedAt: Date.now()
      } as BullMQTaskData,
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      updateProgress: jest.fn().mockResolvedValue(undefined),
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      updateData: jest.fn().mockResolvedValue(undefined)
    };
    
    // 模拟setTimeout
    jest.useFakeTimers();
    
    // 获取processorFn方法（通过一些技巧访问私有方法）
    const processorFn = (workerManager as any).processorFn.bind(workerManager);
    
    // 创建处理Promise
    const processingPromise = processorFn(mockJob);
    
    // 快进时间
    jest.advanceTimersByTime(3000);
    
    // 等待处理完成
    await processingPromise;
    
    // 验证状态更新
    expect(mockJob.updateProgress).toHaveBeenCalledWith(30);
    expect(mockJob.updateProgress).toHaveBeenCalledWith(100);
    expect(mockJob.updateData).toHaveBeenCalledTimes(2);
    
    // 验证第一次更新为"in progress"
    const firstCallArgs = mockJob.updateData.mock.calls[0][0] as BullMQTaskData;
    expect(firstCallArgs.status).toBe('in progress');
    
    // 验证第二次更新为"done"
    const secondCallArgs = mockJob.updateData.mock.calls[1][0] as BullMQTaskData;
    expect(secondCallArgs.status).toBe('done');
    
    // 清理
    jest.useRealTimers();
  });
  
  test('processorFn应正确处理任务失败', async () => {
    // 创建模拟Job对象
    const mockJob = {
      id: 'task-1',
      data: {
        id: 'task-1',
        title: '测试任务',
        description: '测试描述',
        status: 'not started' as const,
        approved: false,
        completedDetails: '',
        projectId: 'proj-1',
        createdAt: Date.now(),
        updatedAt: Date.now()
      } as BullMQTaskData,
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      updateProgress: jest.fn().mockResolvedValue(undefined),
      // @ts-ignore - 忽略Jest模拟函数的返回类型错误
      updateData: jest.fn().mockResolvedValue(undefined)
    };
    
    // 模拟随机数生成以确保总是"失败"
    const originalRandom = Math.random;
    // @ts-ignore - 忽略类型转换错误
    Math.random = jest.fn().mockReturnValue(0.05);
    
    // 获取processorFn方法
    const processorFn = (workerManager as any).processorFn.bind(workerManager);
    
    // 执行处理函数并捕获预期的错误
    await expect(processorFn(mockJob)).rejects.toThrow('模拟的任务失败');
    
    // 验证状态更新为"not started"以允许重试
    expect(mockJob.updateData).toHaveBeenCalledTimes(2);
    
    const firstCallArgs = mockJob.updateData.mock.calls[0][0] as BullMQTaskData;
    const secondCallArgs = mockJob.updateData.mock.calls[1][0] as BullMQTaskData;
    
    expect(firstCallArgs.status).toBe('in progress');
    expect(secondCallArgs.status).toBe('not started');
    expect(secondCallArgs.completedDetails).toContain('模拟的任务失败');
    
    // 恢复原始Math.random
    Math.random = originalRandom;
  });
}); 