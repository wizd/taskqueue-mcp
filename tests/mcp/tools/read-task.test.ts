import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import {
  setupTestContext,
  teardownTestContext,
  verifyToolExecutionError,
  verifyToolSuccessResponse,
  createTestProjectInFile,
  createTestTaskInFile,
  TestContext
} from '../test-helpers.js';
import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { Task } from "../../../src/types/data.js";
import { Redis } from 'ioredis';
import { RedisKeys, MigrationMode } from '../../../src/types/bullmq.js';

// 检测是否在BullMQ模式
const isBullMQMode = process.env.TASK_STORAGE_MODE === 'BULLMQ_ONLY';
console.log(`当前存储模式: ${process.env.TASK_STORAGE_MODE || '默认'}, isBullMQMode = ${isBullMQMode}`);

describe('read_task Tool', () => {
  let context: TestContext;

  beforeAll(async () => {
    context = await setupTestContext();
  });

  afterAll(async () => {
    await teardownTestContext(context);
  });

  describe('Success Cases', () => {
    it('should successfully read an existing task', async () => {
      // 创建一个项目和任务
      const createResult = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Test Project",
          tasks: [] // 不创建任务，将直接使用Redis添加
        }
      }) as CallToolResult;
    
      const createData = JSON.parse((createResult.content[0] as {text: string}).text);
      const projectId = createData.projectId;
      
      // 使用Redis直接添加任务
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      let taskId = '';
      
      const redis = new Redis(redisOptions);
      try {
        // 创建任务
        const queueName = RedisKeys.projectQueueName(projectId);
        taskId = `task-${Date.now()}`;
        const taskData = {
          id: taskId,
          title: "Test Task",
          description: "Task description",
          status: "not started",
          approved: false,
          completedDetails: "",
          projectId
        };
        
        console.log(`正在添加任务 ${taskId} 到项目 ${projectId}`);
        
        // 添加任务到队列
        await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(taskData));
        
        // 添加任务ID到项目任务集合
        await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
        
        // 更新项目任务计数
        await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', 1);
        
        // 等待一会确保数据已同步
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // 验证任务已添加到Redis
        const taskExists = await redis.exists(`bull:${queueName}:${taskId}`);
        console.log(`任务是否存在于Redis: ${taskExists ? '是' : '否'}`);
        
        const taskInSet = await redis.sismember(RedisKeys.projectTasks(projectId), taskId);
        console.log(`任务是否在项目任务集合中: ${taskInSet ? '是' : '否'}`);
      } finally {
        await redis.quit();
      }

      // 使用readTask工具
      const result = await context.client.callTool({
        name: "read_task",
        arguments: {
          projectId: projectId,
          taskId: taskId
        }
      }) as CallToolResult;

      // 验证响应
      expect(result.isError).toBeFalsy();
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      
      expect(responseData).toHaveProperty('task');
      expect(responseData.task).toHaveProperty('id', taskId);
      expect(responseData.task).toHaveProperty('title', "Test Task");
      expect(responseData.task).toHaveProperty('description', "Task description");
      expect(responseData.task).toHaveProperty('status', "not started");
    });

    it('should read a completed task with all details', async () => {
      // 创建一个项目和任务
      const createResult = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Project with Completed Task",
          tasks: [] // 不创建任务，将直接使用Redis添加
        }
      }) as CallToolResult;
    
      const createData = JSON.parse((createResult.content[0] as {text: string}).text);
      const projectId = createData.projectId;
      
      // 使用Redis直接添加任务
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      let taskId = '';
      
      const redis = new Redis(redisOptions);
      try {
        // 创建任务
        const queueName = RedisKeys.projectQueueName(projectId);
        taskId = `task-${Date.now()}`;
        const taskData = {
          id: taskId,
          title: "Completed Task",
          description: "A finished task",
          status: "done",
          approved: true,
          completedDetails: "Task was completed successfully",
          toolRecommendations: "Used tool X and Y",
          ruleRecommendations: "Applied rule Z",
          projectId
        };
        
        console.log(`正在添加已完成的任务 ${taskId} 到项目 ${projectId}`);
        
        // 添加任务到队列
        await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(taskData));
        
        // 添加任务ID到项目任务集合
        await redis.sadd(RedisKeys.projectTasks(projectId), taskId);
        
        // 更新项目任务计数
        await redis.hincrby(RedisKeys.projectMetadata(projectId), 'taskCount', 1);
        
        // 等待一会确保数据已同步
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // 验证任务已添加到Redis
        const taskExists = await redis.exists(`bull:${queueName}:${taskId}`);
        console.log(`任务是否存在于Redis: ${taskExists ? '是' : '否'}`);
        
        const taskInSet = await redis.sismember(RedisKeys.projectTasks(projectId), taskId);
        console.log(`任务是否在项目任务集合中: ${taskInSet ? '是' : '否'}`);
      } finally {
        await redis.quit();
      }

      // 使用readTask工具
      const result = await context.client.callTool({
        name: "read_task",
        arguments: {
          projectId: projectId,
          taskId: taskId
        }
      }) as CallToolResult;

      // 验证响应
      expect(result.isError).toBeFalsy();
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      
      expect(responseData).toHaveProperty('task');
      expect(responseData.task).toHaveProperty('id', taskId);
      expect(responseData.task).toHaveProperty('title', "Completed Task");
      expect(responseData.task).toHaveProperty('description', "A finished task");
      expect(responseData.task).toHaveProperty('status', "done");
      expect(responseData.task).toHaveProperty('approved', true);
      expect(responseData.task).toHaveProperty('completedDetails', "Task was completed successfully");
      expect(responseData.task).toHaveProperty('toolRecommendations', "Used tool X and Y");
      expect(responseData.task).toHaveProperty('ruleRecommendations', "Applied rule Z");
    });
  });

  describe('Error Cases', () => {
    it('should return error for invalid task ID', async () => {
      const result = await context.client.callTool({
        name: "read_task",
        arguments: {
          projectId: "invalid-format",
          taskId: "invalid-task-id"
        }
      }) as CallToolResult;

      // 直接检查错误信息
      expect(result.isError).toBeTruthy();
      expect(result.content.length).toBeGreaterThan(0);
      const errorMessage = (result.content[0] as { text: string })?.text;
      expect(errorMessage).toContain('不存在'); // 至少要检查返回了某种不存在的错误
    });

    it('should return error for non-existent task in existing project', async () => {
      // 创建一个项目但不创建任务
      const createResult = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Test Project",
          tasks: [] // 不创建任务
        }
      }) as CallToolResult;
    
      const createData = JSON.parse((createResult.content[0] as {text: string}).text);
      const projectId = createData.projectId;
      
      const result = await context.client.callTool({
        name: "read_task",
        arguments: {
          projectId: projectId,
          taskId: "non-existent-task"
        }
      }) as CallToolResult;

      // 直接检查错误信息
      expect(result.isError).toBeTruthy();
      expect(result.content.length).toBeGreaterThan(0);
      const errorMessage = (result.content[0] as { text: string })?.text;
      expect(errorMessage).toContain('任务 non-existent-task 不存在');
    });

    it('should return error for invalid task ID format in valid project', async () => {
      // 创建一个项目
      const createResult = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Test Project",
          tasks: [
            {
              title: "Test Task",
              description: "Task description"
            }
          ]
        }
      }) as CallToolResult;
    
      const createData = JSON.parse((createResult.content[0] as {text: string}).text);
      const projectId = createData.projectId;
      
      const result = await context.client.callTool({
        name: "read_task",
        arguments: {
          projectId: projectId,
          taskId: "invalid-task-id"
        }
      }) as CallToolResult;

      // 直接检查错误信息
      expect(result.isError).toBeTruthy();
      expect(result.content.length).toBeGreaterThan(0);
      const errorMessage = (result.content[0] as { text: string })?.text;
      expect(errorMessage).toContain('任务 invalid-task-id 不存在');
    });
  });
}); 