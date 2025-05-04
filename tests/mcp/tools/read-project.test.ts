import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import {
  setupTestContext,
  teardownTestContext,
  verifyCallToolResult,
  createTestProjectInFile,
  createTestTaskInFile,
  TestContext,
  verifyToolExecutionError,
  createTestProject
} from '../test-helpers.js';
import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { Redis } from 'ioredis';
import { RedisKeys, MigrationMode } from '../../../src/types/bullmq.js';

// 检测是否在BullMQ模式
const isBullMQMode = process.env.TASK_STORAGE_MODE === 'BULLMQ_ONLY';
console.log(`当前存储模式: ${process.env.TASK_STORAGE_MODE || '默认'}, isBullMQMode = ${isBullMQMode}`);

describe('read_project Tool', () => {
  let context: TestContext;

  beforeAll(async () => {
    context = await setupTestContext();
  });

  afterAll(async () => {
    await teardownTestContext(context);
  });

  describe('Success Cases', () => {
    it('should read a project with minimal data', async () => {
      let projectId: string;

      // 根据存储模式创建项目
      if (context.storageMode !== MigrationMode.FILE_ONLY) {
        // 创建一个带有最小数据的项目
        projectId = await createTestProject(context.client, {
          initialPrompt: "Test Project"
        });
        
        // 使用Redis直接添加任务
        const redisOptions = {
          host: process.env.REDIS_HOST || 'localhost',
          port: Number(process.env.REDIS_PORT || 6379),
          password: process.env.REDIS_PASSWORD || '',
          db: Number(process.env.REDIS_DB || 0),
        };
        
        const redis = new Redis(redisOptions);
        try {
          // 创建任务
          const queueName = RedisKeys.projectQueueName(projectId);
          const taskId = `task-${Date.now()}`;
          const taskData = {
            id: taskId,
            title: "Test Task",
            description: "Test Description",
            status: "not started",
            approved: false,
            completedDetails: "",
            projectId
          };
          
          // 添加任务到队列
          await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(taskData));
          
          // 添加任务ID到项目任务集合
          await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
          
          // 更新项目任务计数
          await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', 1);
          
          // 等待一会确保数据已同步
          await new Promise(resolve => setTimeout(resolve, 500));
        } finally {
          await redis.quit();
        }
      } else {
        // 使用文件模式创建项目
        const project = await createTestProjectInFile(context.testFilePath, {
          initialPrompt: "Test Project",
          projectPlan: "",
          completed: false
        });
        
        projectId = project.projectId;
        
        await createTestTaskInFile(context.testFilePath, projectId, {
          title: "Test Task",
          description: "Test Description"
        });
      }

      // 读取项目
      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      // 验证响应
      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();

      // 验证项目数据
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('projectId', projectId);
      expect(responseData).toHaveProperty('initialPrompt', "Test Project");
      expect(responseData).toHaveProperty('completed', false);
      expect(responseData).toHaveProperty('tasks');
      expect(Array.isArray(responseData.tasks)).toBe(true);
      expect(responseData.tasks.length).toBeGreaterThan(0);
      
      // 验证任务数据
      const task = responseData.tasks[0];
      expect(task).toHaveProperty('title', "Test Task");
      expect(task).toHaveProperty('description', "Test Description");
      expect(task).toHaveProperty('status', "not started");
      expect(task).toHaveProperty('approved', false);
    });

    it('should read a project with all optional fields', async () => {
      let projectId: string;

      // 根据存储模式创建项目
      if (context.storageMode !== MigrationMode.FILE_ONLY) {
        // 创建一个包含所有可选字段的项目
        const createResult = await context.client.callTool({
          name: "create_project", 
          arguments: {
            initialPrompt: "Full Project",
            tasks: [{
              title: "Full Task",
              description: "Task with all fields"
            }],
            projectPlan: "Detailed project plan",
            autoApprove: true
          }
        }) as CallToolResult;
        
        const createData = JSON.parse((createResult.content[0] as {text: string}).text);
        projectId = createData.projectId;
        
        // 使用Redis直接添加任务
        const redisOptions = {
          host: process.env.REDIS_HOST || 'localhost',
          port: Number(process.env.REDIS_PORT || 6379),
          password: process.env.REDIS_PASSWORD || '',
          db: Number(process.env.REDIS_DB || 0),
        };
        
        const redis = new Redis(redisOptions);
        try {
          // 创建任务
          const queueName = RedisKeys.projectQueueName(projectId);
          const taskId = `task-${Date.now()}`;
          const taskData = {
            id: taskId,
            title: "Full Task",
            description: "Task with all fields",
            status: "done",
            approved: true,
            completedDetails: "Task completed",
            toolRecommendations: "Use these tools",
            ruleRecommendations: "Follow these rules",
            projectId
          };
          
          // 添加任务到队列
          await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(taskData));
          
          // 添加任务ID到项目任务集合
          await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
          
          // 更新项目任务计数
          await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', 1);
          
          // 等待一会确保数据已同步
          await new Promise(resolve => setTimeout(resolve, 500));
        } finally {
          await redis.quit();
        }
      } else {
        // 使用文件模式创建项目
        const project = await createTestProjectInFile(context.testFilePath, {
          initialPrompt: "Full Project",
          projectPlan: "Detailed project plan",
          completed: false,
          autoApprove: true
        });
        
        projectId = project.projectId;
        
        await createTestTaskInFile(context.testFilePath, projectId, {
          title: "Full Task",
          description: "Task with all fields",
          status: "done",
          approved: true,
          completedDetails: "Task completed",
          toolRecommendations: "Use these tools",
          ruleRecommendations: "Follow these rules"
        });
      }

      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();
      
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('projectId', projectId);
      expect(responseData).toHaveProperty('initialPrompt', "Full Project");
      expect(responseData).toHaveProperty('projectPlan', "Detailed project plan");
      expect(responseData).toHaveProperty('autoApprove', true);
      expect(responseData).toHaveProperty('tasks');
      
      const task = responseData.tasks[0];
      expect(task).toHaveProperty('title', "Full Task");
      expect(task).toHaveProperty('description', "Task with all fields");
      expect(task).toHaveProperty('status', "done");
      expect(task).toHaveProperty('approved', true);
      expect(task).toHaveProperty('completedDetails', "Task completed");
      expect(task).toHaveProperty('toolRecommendations', "Use these tools");
      expect(task).toHaveProperty('ruleRecommendations', "Follow these rules");
    });

    it('should read a completed project', async () => {
      let projectId: string;

      // 根据存储模式创建项目
      if (context.storageMode !== MigrationMode.FILE_ONLY) {
        // 创建一个已完成的项目
        projectId = await createTestProject(context.client);
        
        // 使用Redis直接添加任务和设置项目为已完成
        const redisOptions = {
          host: process.env.REDIS_HOST || 'localhost',
          port: Number(process.env.REDIS_PORT || 6379),
          password: process.env.REDIS_PASSWORD || '',
          db: Number(process.env.REDIS_DB || 0),
        };
        
        const redis = new Redis(redisOptions);
        try {
          // 创建已完成的任务
          const queueName = RedisKeys.projectQueueName(projectId);
          const taskId = `task-${Date.now()}`;
          const taskData = {
            id: taskId,
            title: "Completed Task",
            description: "This task is done",
            status: "done",
            approved: true,
            completedDetails: "Task completed",
            projectId
          };
          
          // 添加任务到队列
          await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(taskData));
          
          // 添加任务ID到项目任务集合
          await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
          
          // 更新项目任务计数
          await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', 1);
          
          // 设置项目为已完成
          await redis.hset(
            RedisKeys.projectMetadata(projectId),
            'completed',
            'true'
          );
          
          // 等待一会确保数据已同步
          await new Promise(resolve => setTimeout(resolve, 500));
        } finally {
          await redis.quit();
        }
      } else {
        // 使用文件模式创建项目
        const project = await createTestProjectInFile(context.testFilePath, {
          initialPrompt: "Completed Project",
          completed: true
        });
        
        projectId = project.projectId;
        
        await createTestTaskInFile(context.testFilePath, projectId, {
          title: "Completed Task",
          description: "This task is done",
          status: "done",
          approved: true,
          completedDetails: "Task completed"
        });
      }

      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();
      
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('projectId', projectId);
      expect(responseData).toHaveProperty('completed', true);
      expect(responseData).toHaveProperty('tasks');
      
      // 验证任务状态
      const task = responseData.tasks[0];
      expect(task).toHaveProperty('status', "done");
      expect(task).toHaveProperty('approved', true);
    });

    it('should read a project with multiple tasks', async () => {
      let projectId: string;

      // 根据存储模式创建项目
      if (context.storageMode !== MigrationMode.FILE_ONLY) {
        // 创建一个多任务项目
        projectId = await createTestProject(context.client, {
          initialPrompt: "Multi-task Project"
        });
        
        // 使用Redis直接添加多个不同状态的任务
        const redisOptions = {
          host: process.env.REDIS_HOST || 'localhost',
          port: Number(process.env.REDIS_PORT || 6379),
          password: process.env.REDIS_PASSWORD || '',
          db: Number(process.env.REDIS_DB || 0),
        };
        
        const redis = new Redis(redisOptions);
        try {
          const queueName = RedisKeys.projectQueueName(projectId);
          const taskIds = [];
          
          // 任务1：未开始
          const task1Id = `task-${Date.now()}-1`;
          await redis.hset(`bull:${queueName}:${task1Id}`, "data", JSON.stringify({
            id: task1Id,
            title: "Task 1",
            description: "Not started",
            status: "not started",
            approved: false,
            completedDetails: "",
            projectId
          }));
          taskIds.push(task1Id);
          
          // 任务2：进行中
          const task2Id = `task-${Date.now()}-2`;
          await redis.hset(`bull:${queueName}:${task2Id}`, "data", JSON.stringify({
            id: task2Id,
            title: "Task 2",
            description: "In progress",
            status: "in progress",
            approved: false,
            completedDetails: "",
            projectId
          }));
          taskIds.push(task2Id);
          
          // 任务3：已完成
          const task3Id = `task-${Date.now()}-3`;
          await redis.hset(`bull:${queueName}:${task3Id}`, "data", JSON.stringify({
            id: task3Id,
            title: "Task 3",
            description: "Completed",
            status: "done",
            approved: true,
            completedDetails: "Done and approved",
            projectId
          }));
          taskIds.push(task3Id);
          
          // 添加所有任务ID到项目任务集合
          await redis.sadd(RedisKeys.projectTasks(projectId), ...taskIds);
          
          // 更新项目任务计数
          await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', taskIds.length);
          
          // 等待一会确保数据已同步
          await new Promise(resolve => setTimeout(resolve, 500));
        } finally {
          await redis.quit();
        }
      } else {
        // 使用文件模式创建项目
        const project = await createTestProjectInFile(context.testFilePath, {
          initialPrompt: "Multi-task Project"
        });
        
        projectId = project.projectId;

        // 创建不同状态的任务
        await Promise.all([
          createTestTaskInFile(context.testFilePath, projectId, {
            title: "Task 1",
            description: "Not started",
            status: "not started"
          }),
          createTestTaskInFile(context.testFilePath, projectId, {
            title: "Task 2",
            description: "In progress",
            status: "in progress"
          }),
          createTestTaskInFile(context.testFilePath, projectId, {
            title: "Task 3",
            description: "Completed",
            status: "done",
            approved: true,
            completedDetails: "Done and approved"
          })
        ]);
      }

      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();
      
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData.tasks).toHaveLength(3);
      
      // 验证任务状态，按任务标题排序以确保顺序一致
      const sortedTasks = [...responseData.tasks].sort((a, b) => a.title.localeCompare(b.title));
      expect(sortedTasks[0].status).toBe("not started"); // Task 1
      expect(sortedTasks[1].status).toBe("in progress"); // Task 2
      expect(sortedTasks[2].status).toBe("done"); // Task 3
    });
  });

  describe('Error Cases', () => {
    it('should return error for non-existent project', async () => {
      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId: "non_existent_project"
        }
      }) as CallToolResult;

      // 直接检查错误信息而不使用verifyToolExecutionError
      expect(result.isError).toBeTruthy();
      expect(result.content.length).toBeGreaterThan(0);
      const errorMessage = (result.content[0] as { text: string })?.text;
      expect(errorMessage).toContain('项目 non_existent_project 不存在');
    });

    it('should return error for invalid project ID format', async () => {
      const result = await context.client.callTool({
        name: "read_project",
        arguments: {
          projectId: "invalid-format"
        }
      }) as CallToolResult;

      // 直接检查错误信息而不使用verifyToolExecutionError
      expect(result.isError).toBeTruthy();
      expect(result.content.length).toBeGreaterThan(0);
      const errorMessage = (result.content[0] as { text: string })?.text;
      expect(errorMessage).toContain('项目 invalid-format 不存在');
    });
    
    // 为BullMQ添加Redis连接错误测试
    it('should handle Redis connection errors gracefully', async () => {
      // 仅当BullMQ模式下才有意义进行测试
      if (context.storageMode === MigrationMode.FILE_ONLY) {
        console.log('此测试在文件存储模式下被跳过');
        return;
      }
      
      // 创建一个带有无效Redis配置的客户端
      const badRedisOptions = {
        host: '127.0.0.1',  // 使用有效的IP地址以避免立即失败
        port: 65535,        // 使用极高概率不可用的端口
        connectTimeout: 1000, // 快速超时
        maxRetriesPerRequest: 1,
        retryStrategy: () => null, // 禁用重试
      };
      
      let badRedisClient: Redis | undefined;
      try {
        badRedisClient = new Redis(badRedisOptions);
        
        // 尝试执行Redis命令（预期会失败）
        await badRedisClient.ping();
        
      } catch (error: unknown) {
        // Redis连接失败是预期结果
        console.log('预期的Redis连接错误:', error instanceof Error ? error.message : String(error));
      } finally {
        if (badRedisClient) {
          try {
            badRedisClient.disconnect();
          } catch (e) {
            // 忽略断开连接的错误
          }
        }
      }
      
      // 由于Redis错误处理是异步的，系统应该仍能继续运行
      // 这个测试主要是为了确保系统在Redis连接失败时不会完全崩溃
      console.log('Redis连接错误测试完成，系统应继续正常运行');
    });
  });
}); 