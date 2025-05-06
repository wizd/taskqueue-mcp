import { startBullBoard, addQueueToBoard, removeQueueFromBoard } from '../../src/server/bullBoardMonitor.js';
import { RedisManager } from '../../src/server/RedisManager.js';
import { BullMQService } from '../../src/server/BullMQService.js';
import { RedisKeys } from '../../src/types/bullmq.js';
import { AppError, AppErrorCode } from '../../src/types/errors.js';

// 模拟依赖
jest.mock('express', () => {
  const mockApp = {
    use: jest.fn(),
    listen: jest.fn((port, callback) => {
      callback();
      return {
        close: jest.fn()
      };
    })
  };
  return jest.fn(() => mockApp);
});

jest.mock('@bull-board/api', () => {
  return {
    createBullBoard: jest.fn(() => {
      return {
        addQueue: jest.fn(),
        removeQueue: jest.fn(),
        setQueues: jest.fn()
      };
    })
  };
});

jest.mock('@bull-board/express', () => {
  return {
    ExpressAdapter: jest.fn().mockImplementation(() => {
      return {
        setBasePath: jest.fn(),
        getRouter: jest.fn(() => ({}))
      };
    })
  };
});

jest.mock('../../src/server/RedisManager.js', () => {
  const mockRedisInstance = {
    keys: jest.fn().mockResolvedValue(['project:proj-1:metadata', 'project:proj-2:metadata']),
    hset: jest.fn().mockResolvedValue(1),
    duplicate: jest.fn().mockReturnValue({
      subscribe: jest.fn().mockResolvedValue(true),
      on: jest.fn()
    }),
    config: jest.fn().mockResolvedValue('OK')
  };

  return {
    RedisManager: {
      getInstance: jest.fn().mockReturnValue({
        initialize: jest.fn().mockResolvedValue(true),
        getConnection: jest.fn().mockReturnValue(mockRedisInstance)
      })
    }
  };
});

jest.mock('bullmq', () => {
  return {
    Queue: jest.fn().mockImplementation(() => {
      return {};
    }),
    QueueEvents: jest.fn().mockImplementation(() => {
      return {};
    })
  };
});

// 测试开始
describe('Bull Board Monitor', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('应该成功启动Bull Board并初始扫描现有队列', async () => {
    const { createBullBoard } = require('@bull-board/api');
    
    await startBullBoard(3000, '/bull-board');
    
    // 验证Redis查询了项目元数据
    const redis = RedisManager.getInstance().getConnection();
    expect(redis.keys).toHaveBeenCalledWith(RedisKeys.projectMetadataPattern());
    
    // 验证创建了Bull Board
    expect(createBullBoard).toHaveBeenCalled();
    expect(createBullBoard.mock.calls[0][0].queues.length).toBe(2); // 两个测试队列
  });

  it('应该能够手动添加队列到Bull Board', async () => {
    const { createBullBoard } = require('@bull-board/api');
    
    // 首先启动Bull Board
    await startBullBoard(3000, '/bull-board');
    
    // 确保createBullBoard被调用，并返回addQueue方法
    expect(createBullBoard).toHaveBeenCalled();
    const returnedAPI = createBullBoard.mock.results[0].value;
    expect(returnedAPI.addQueue).toBeDefined();
    
    // 模拟手动添加队列
    await addQueueToBoard('proj-3');
    
    // 验证addQueue被调用
    expect(returnedAPI.addQueue).toHaveBeenCalledTimes(1);
  });

  it('应该能够从Bull Board中移除队列', async () => {
    const { createBullBoard } = require('@bull-board/api');
    
    // 首先启动Bull Board
    await startBullBoard(3000, '/bull-board');
    
    // 确保createBullBoard被调用，并返回removeQueue方法
    expect(createBullBoard).toHaveBeenCalled();
    const returnedAPI = createBullBoard.mock.results[0].value;
    expect(returnedAPI.removeQueue).toBeDefined();
    
    // 模拟移除队列（首先需要添加一个队列）
    await addQueueToBoard('proj-3');
    await removeQueueFromBoard('proj-3');
    
    // 验证removeQueue被调用
    expect(returnedAPI.removeQueue).toHaveBeenCalledTimes(1);
  });

  it('应该在没有初始化时拒绝添加队列', async () => {
    // 重置状态，通过重新加载模块
    jest.resetModules();
    const { addQueueToBoard } = require('../../src/server/bullBoardMonitor.js');
    
    // 尝试在Bull Board启动前添加队列
    await expect(addQueueToBoard('proj-4')).rejects.toThrow(AppError);
  });

  it('应该在没有初始化时优雅地处理移除队列请求（不抛出错误）', async () => {
    // 重置状态，通过重新加载模块
    jest.resetModules();
    const { removeQueueFromBoard } = require('../../src/server/bullBoardMonitor.js');
    
    // 尝试在Bull Board启动前移除队列
    await expect(removeQueueFromBoard('proj-4')).resolves.not.toThrow();
  });
}); 