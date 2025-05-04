import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import {
  setupTestContext,
  teardownTestContext,
  verifyToolExecutionError,
  verifyToolSuccessResponse,
  createTestProject,
  TestContext
} from '../test-helpers.js';
import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { Redis } from 'ioredis';
import { RedisKeys } from '../../../src/types/bullmq.js';

describe('delete_project Tool', () => {
  let context: TestContext;

  beforeEach(async () => {
    context = await setupTestContext();
  });

  afterEach(async () => {
    await teardownTestContext(context);
  });

  describe('Success Cases', () => {
    it('should successfully delete an empty project', async () => {
      // Create a project using the actual create_project tool
      const projectId = await createTestProject(context.client, {
        initialPrompt: "Test Project",
        tasks: []  // No tasks
      });

      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolSuccessResponse(result);

      // Verify project is deleted by attempting to read a task from it
      const readResult = await context.client.callTool({
        name: "get_next_task",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolExecutionError(readResult, /项目 .* 不存在/);
    });

    it('should successfully delete a project with non-approved tasks', async () => {
      // Create a project with non-approved tasks using the actual create_project tool
      const projectId = await createTestProject(context.client, {
        initialPrompt: "Test Project with Tasks",
        tasks: [
          { title: "Task 1", description: "First task" },
          { title: "Task 2", description: "Second task" }
        ]
      });

      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolSuccessResponse(result);

      // Verify project is deleted
      const readResult = await context.client.callTool({
        name: "get_next_task",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolExecutionError(readResult, /项目 .* 不存在/);
    });

    it('should successfully delete a project with approved tasks', async () => {
      // Create a project with tasks
      const projectId = await createTestProject(context.client, {
        initialPrompt: "Project with Tasks",
        tasks: [
          { title: "Task to Approve", description: "This task will be approved" }
        ]
      });

      // 直接使用Redis获取任务ID
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      const redis = new Redis(redisOptions);
      let taskId;
      try {
        // 获取项目中的任务ID列表
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        expect(taskIds.length).toBe(1);
        taskId = taskIds[0];
      } finally {
        await redis.quit();
      }

      // Mark task as done
      await context.client.callTool({
        name: "update_task",
        arguments: {
          projectId,
          taskId,
          status: "done",
          completedDetails: "Task completed"
        }
      });

      // Approve the task
      await context.client.callTool({
        name: "approve_task",
        arguments: {
          projectId,
          taskId
        }
      });

      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolSuccessResponse(result);

      // Verify project is deleted
      const readResult = await context.client.callTool({
        name: "get_next_task",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolExecutionError(readResult, /项目 .* 不存在/);
    });

    it('should successfully delete a completed project', async () => {
      // Create a project and complete all its tasks
      const projectId = await createTestProject(context.client, {
        initialPrompt: "Project to Complete",
        tasks: [
          { title: "Task 1", description: "Task to complete" }
        ]
      });

      // 直接使用Redis获取任务ID
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      const redis = new Redis(redisOptions);
      let taskId;
      try {
        // 获取项目中的任务ID列表
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        expect(taskIds.length).toBe(1);
        taskId = taskIds[0];
      } finally {
        await redis.quit();
      }

      // Mark task as done
      await context.client.callTool({
        name: "update_task",
        arguments: {
          projectId,
          taskId,
          status: "done",
          completedDetails: "Task completed"
        }
      });

      // Approve the task
      await context.client.callTool({
        name: "approve_task",
        arguments: {
          projectId,
          taskId
        }
      });

      // Mark project as completed
      await context.client.callTool({
        name: "finalize_project",
        arguments: {
          projectId
        }
      });

      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolSuccessResponse(result);

      // Verify project is deleted
      const readResult = await context.client.callTool({
        name: "get_next_task",
        arguments: {
          projectId
        }
      }) as CallToolResult;

      verifyToolExecutionError(readResult, /项目 .* 不存在/);
    });
  });

  describe('Error Cases', () => {
    it('should return error for non-existent project', async () => {
      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId: "non_existent_project"
        }
      }) as CallToolResult;

      verifyToolExecutionError(result, /项目 non_existent_project 不存在/);
    });

    it('should return error for invalid project ID format', async () => {
      const result = await context.client.callTool({
        name: "delete_project",
        arguments: {
          projectId: "invalid-format"
        }
      }) as CallToolResult;

      verifyToolExecutionError(result, /项目 invalid-format 不存在/);
    });
  });
}); 