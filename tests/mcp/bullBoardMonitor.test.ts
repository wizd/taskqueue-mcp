import { setupBullBoard, addQueueToBoard, removeQueueFromBoard } from '../../src/server/bullBoardMonitor.js';
import { RedisManager } from '../../src/server/RedisManager.js';
import { BullMQService } from '../../src/server/BullMQService.js';
import { createRedisKeys } from '../../src/types/bullmq.js';
import { AppError, AppErrorCode } from '../../src/types/errors.js';
import express from 'express';

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

let mockBullBoardApi = {
  addQueue: jest.fn(),
  removeQueue: jest.fn(),
  setQueues: jest.fn()
};

jest.mock('@bull-board/api', () => {
  return {
    createBullBoard: jest.fn(() => {
      return mockBullBoardApi;
    })
  };
});

let mockExpressAdapterInstance = {
  setBasePath: jest.fn(),
  getRouter: jest.fn(() => ({}))
};

jest.mock('@bull-board/express', () => {
  return {
    ExpressAdapter: jest.fn().mockImplementation(() => {
      return mockExpressAdapterInstance;
    })
  };
});

let mockRedisInstance = {
  keys: jest.fn().mockResolvedValue(['project:proj-1:metadata', 'project:proj-2:metadata']),
  hset: jest.fn().mockResolvedValue(1),
  duplicate: jest.fn().mockReturnValue({
    subscribe: jest.fn().mockResolvedValue(true),
    on: jest.fn()
  }),
  config: jest.fn().mockResolvedValue('OK')
};

jest.mock('../../src/server/RedisManager.js', () => {
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
  let mockApp: express.Application;
  let ExpressAdapterMock: jest.Mock;
  let createBullBoardMock: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
    const expressMock = require('express');
    mockApp = expressMock();
    ExpressAdapterMock = require('@bull-board/express').ExpressAdapter;
    createBullBoardMock = require('@bull-board/api').createBullBoard;
    mockBullBoardApi.addQueue.mockClear();
    mockBullBoardApi.removeQueue.mockClear();
    mockExpressAdapterInstance.setBasePath.mockClear();
    mockRedisInstance.keys.mockClear();
  });

  it('应该成功初始化Bull Board并尝试发现队列', () => {
    const basePath = '/bull-board';
    setupBullBoard(mockApp as express.Express, basePath);

    expect(ExpressAdapterMock).toHaveBeenCalledTimes(1);
    expect(mockExpressAdapterInstance.setBasePath).toHaveBeenCalledWith(basePath);
    
    expect(createBullBoardMock).toHaveBeenCalledTimes(1);
    
    const defaultRedisKeys = createRedisKeys();
    expect(mockRedisInstance.keys).toHaveBeenCalledWith(defaultRedisKeys.projectMetadataPattern());
    
    expect(mockApp.use).toHaveBeenCalledWith(basePath, expect.any(Object));
  });

  it('应该能够在初始化后手动添加队列到Bull Board', async () => {
    setupBullBoard(mockApp as express.Express, '/bull-board');
    
    await addQueueToBoard('proj-3');
    
    expect(mockBullBoardApi.addQueue).toHaveBeenCalledTimes(1);
  });

  it('应该能够在初始化后从Bull Board中移除队列', async () => {
    setupBullBoard(mockApp as express.Express, '/bull-board');

    const projectId = 'proj-3';
    const prefix = 'tenant:test:';
    const redisKeys = createRedisKeys(prefix);
    const queueName = redisKeys.projectQueueName(projectId);

    await addQueueToBoard(projectId, prefix);
    await removeQueueFromBoard(projectId, prefix);
    
    expect(mockBullBoardApi.removeQueue).toHaveBeenCalledTimes(1);
  });

  it('应该在没有初始化时将添加队列请求放入待处理列表 (不抛出错误)', async () => {
    jest.resetModules();
    const { addQueueToBoard } = require('../../src/server/bullBoardMonitor.js');
    const mockRedisMgr = require('../../src/server/RedisManager.js').RedisManager;
    mockRedisMgr.getInstance().getConnection();
    
    await expect(addQueueToBoard('proj-4')).resolves.not.toThrow();
  });

  it('应该在没有初始化时优雅地处理移除队列请求（不抛出错误）', async () => {
    jest.resetModules();
    const { removeQueueFromBoard } = require('../../src/server/bullBoardMonitor.js');
    const mockRedisMgr = require('../../src/server/RedisManager.js').RedisManager;
    mockRedisMgr.getInstance().getConnection();
    
    await expect(removeQueueFromBoard('proj-4')).resolves.not.toThrow();
  });
}); 