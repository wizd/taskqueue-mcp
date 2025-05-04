import { describe, it, expect, beforeEach } from '@jest/globals';
import { 
  setupTestContext, 
  teardownTestContext, 
  TestContext, 
  createTestProject, 
  verifyCallToolResult, 
  verifyTask, 
  verifyToolExecutionError, 
  verifyProtocolError 
} from '../test-helpers.js';
import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';

describe('add_tasks_to_project Tool', () => {
  let context: TestContext;
  let projectId: string;

  beforeEach(async () => {
    context = await setupTestContext();
    // Create a test project for each test case
    projectId = await createTestProject(context.client);
  });

  afterEach(async () => {
    await teardownTestContext(context);
  });

  describe('Success Cases', () => {
    it('should add a single task to project', async () => {
      const result = await context.client.callTool({
        name: "add_tasks_to_project",
        arguments: {
          projectId,
          tasks: [
            { title: "New Task", description: "A task to add" }
          ]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();

      // Parse and verify response
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('message');
      expect(responseData).toHaveProperty('newTasks');
      expect(responseData.newTasks).toHaveLength(1);
      const newTask = responseData.newTasks[0];

      // Verify task was added using BullMQ verification
      await verifyTask(context, projectId, newTask.id, {
        title: "New Task",
        description: "A task to add",
        status: "not started",
        approved: false
      });
    });

    it('should add multiple tasks to project', async () => {
      const tasks = [
        { title: "Task 1", description: "First task to add" },
        { title: "Task 2", description: "Second task to add" },
        { title: "Task 3", description: "Third task to add" }
      ];

      const result = await context.client.callTool({
        name: "add_tasks_to_project",
        arguments: {
          projectId,
          tasks
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData.newTasks).toHaveLength(3);

      // Verify all tasks were added
      for (let i = 0; i < tasks.length; i++) {
        await verifyTask(context, projectId, responseData.newTasks[i].id, {
          title: tasks[i].title,
          description: tasks[i].description,
          status: "not started"
        });
      }
    });

    it('should add tasks with tool and rule recommendations', async () => {
      const result = await context.client.callTool({
        name: "add_tasks_to_project",
        arguments: {
          projectId,
          tasks: [{
            title: "Task with Recommendations",
            description: "Task with specific recommendations",
            toolRecommendations: "Use tool A and B",
            ruleRecommendations: "Follow rules X and Y"
          }]
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      const newTask = responseData.newTasks[0];
      
      await verifyTask(context, projectId, newTask.id, {
        title: "Task with Recommendations",
        description: "Task with specific recommendations",
        toolRecommendations: "Use tool A and B",
        ruleRecommendations: "Follow rules X and Y"
      });
    });

    it('should handle empty tasks array', async () => {
      const result = await context.client.callTool({
        name: "add_tasks_to_project",
        arguments: {
          projectId,
          tasks: []
        }
      }) as CallToolResult;

      verifyCallToolResult(result);
      expect(result.isError).toBeFalsy();
      const responseData = JSON.parse((result.content[0] as { text: string }).text);
      expect(responseData.newTasks).toHaveLength(0);
    });
  });

  describe('Error Cases', () => {
    it('should return error for missing required parameters', async () => {
      try {
        await context.client.callTool({
          name: "add_tasks_to_project",
          arguments: {
            projectId
            // Missing tasks array
          }
        });
        expect(true).toBe(false); // This line should never be reached
      } catch (error) {
        verifyProtocolError(error, -32602, 'Invalid or missing required parameter');
      }
    });

    it('should return error for invalid project ID', async () => {
      const result = await context.client.callTool({
        name: "add_tasks_to_project",
        arguments: {
          projectId: "non-existent-project",
          tasks: [{ title: "Test Task", description: "Test Description" }]
        }
      }) as CallToolResult;

      verifyToolExecutionError(result, /项目 non-existent-project 不存在/);
    });

    it('should return error for task with empty title', async () => {
      try {
        await context.client.callTool({
          name: "add_tasks_to_project",
          arguments: {
            projectId,
            tasks: [{ title: "", description: "Test Description" }]
          }
        });
        expect(true).toBe(false); // This line should never be reached
      } catch (error) {
        verifyProtocolError(error, -32602, 'Invalid or missing required parameter: title');
      }
    });

    it('should return error for task with empty description', async () => {
      try {
        await context.client.callTool({
          name: "add_tasks_to_project",
          arguments: {
            projectId,
            tasks: [{ title: "Test Task", description: "" }]
          }
        });
        expect(true).toBe(false); // This line should never be reached
      } catch (error) {
        verifyProtocolError(error, -32602, 'Invalid or missing required parameter: description');
      }
    });
  });
}); 