import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import {
  setupTestContext,
  teardownTestContext,
  verifyCallToolResult,
  verifyProject,
  verifyTask,
  TestContext,
  verifyProjectInBullMQNative,
  verifyTaskInBullMQNative
} from '../test-helpers.js';
import { CallToolResult, McpError } from '@modelcontextprotocol/sdk/types.js';
import { Redis } from 'ioredis';
import { RedisKeys } from '../../../src/types/bullmq.js';

describe('create_project Tool', () => {
  let context: TestContext;

  beforeAll(async () => {
    context = await setupTestContext();
  });

  afterAll(async () => {
    await teardownTestContext(context);
  });

  describe('Success Cases', () => {
    it('should create a project with minimal parameters', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Test Project",
          tasks: [
            { title: "Task 1", description: "First test task" }
          ]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();

      // Parse and verify response
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('projectId');
      const projectId = responseData.projectId;

      // Verify project was created
      await verifyProject(context, projectId, {
        initialPrompt: "Test Project",
        completed: false
      });

      // Verify task was created
      await verifyTask(context, projectId, responseData.tasks[0].id, {
        title: "Task 1",
        description: "First test task",
        status: "not started",
        approved: false
      });
    });

    it('should create a project with no tasks', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Project with No Tasks",
          tasks: []
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();

      // Parse and verify response
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('projectId');
      const projectId = responseData.projectId;

      // Verify project was created
      await verifyProject(context, projectId, {
        initialPrompt: "Project with No Tasks",
        completed: false
      });

      // Verify no tasks were created
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      const redis = new Redis(redisOptions);
      try {
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        expect(taskIds.length).toBe(0);
      } finally {
        await redis.quit();
      }
    });

    it('should create a project with multiple tasks', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Multi-task Project",
          tasks: [
            { title: "Task 1", description: "First task" },
            { title: "Task 2", description: "Second task" },
            { title: "Task 3", description: "Third task" }
          ]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      const projectId = responseData.projectId;

      // Verify all tasks were created
      const redisOptions = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        password: process.env.REDIS_PASSWORD || '',
        db: Number(process.env.REDIS_DB || 0),
      };
      
      const redis = new Redis(redisOptions);
      try {
        // Verify tasks count
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        expect(taskIds.length).toBe(3);
        
        // Verify project auto-approve
        const projectData = await redis.hgetall(RedisKeys.projectMetadata(projectId));
        expect(projectData.autoApprove).toBe('true');
        
        // Verify individual tasks
        for (let i = 0; i < responseData.tasks.length; i++) {
          const taskId = responseData.tasks[i].id;
          await verifyTask(context, projectId, taskId, {
            title: `Task ${i+1}`,
            description: [`First task`, `Second task`, `Third task`][i],
            status: "not started"
          });
        }
      } finally {
        await redis.quit();
      }
    });

    it('should create a project with auto-approve enabled', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Auto-approve Project",
          tasks: [
            { title: "Auto Task", description: "This task will be auto-approved" }
          ],
          autoApprove: true
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      const projectId = responseData.projectId;

      // Verify project was created with auto-approve
      await verifyProject(context, projectId, {
        initialPrompt: "Auto-approve Project",
        autoApprove: true
      });
    });

    it('should create a project with project plan', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Planned Project",
          projectPlan: "Detailed plan for the project execution",
          tasks: [
            { title: "Planned Task", description: "Task with a plan" }
          ]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      const projectId = responseData.projectId;

      await verifyProject(context, projectId, {
        initialPrompt: "Planned Project",
        projectPlan: "Detailed plan for the project execution"
      });
    });

    it('should create tasks with tool and rule recommendations', async () => {
      const result = await context.client.callTool({
        name: "create_project",
        arguments: {
          initialPrompt: "Project with Recommendations",
          tasks: [{
            title: "Task with Recommendations",
            description: "Task description",
            toolRecommendations: "Use tool X and Y",
            ruleRecommendations: "Follow rules A and B"
          }]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      const projectId = responseData.projectId;
      const taskId = responseData.tasks[0].id;

      await verifyTask(context, projectId, taskId, {
        toolRecommendations: "Use tool X and Y",
        ruleRecommendations: "Follow rules A and B"
      });
    });
  });

  describe('Error Cases', () => {
    it('should return error for missing required parameters', async () => {
      try {
        await context.client.callTool({
          name: "create_project",
          arguments: {
            // Missing initialPrompt and tasks
          }
        });
        fail('Expected McpError to be thrown');
      } catch (error) {
        expect(error instanceof McpError).toBe(true);
        expect((error as McpError).message).toContain('Invalid or missing required parameter: initialPrompt');
      }
    });

    it('should return error for invalid task data', async () => {
      try {
        await context.client.callTool({
          name: "create_project",
          arguments: {
            initialPrompt: "Invalid Task Project",
            tasks: [
              { title: "Task 1" } // Missing required description
            ]
          }
        });
        fail('Expected McpError to be thrown');
      } catch (error) {
        expect(error instanceof McpError).toBe(true);
        expect((error as McpError).message).toContain('Invalid or missing required parameter: description');
      }
    });
  });
}); 