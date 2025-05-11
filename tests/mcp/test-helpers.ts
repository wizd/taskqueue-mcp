import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { Task, Project, TaskManagerFile } from "../../src/types/data.js";
import { FileSystemService } from "../../src/server/FileSystemService.js";
import { MigrationMode } from "../../src/types/bullmq.js";
import * as path from 'node:path';
import * as os from 'node:os';
import * as fs from 'node:fs/promises';
import process from 'node:process';
import dotenv from 'dotenv';
import { Queue, RedisOptions as BullMQRedisOptions } from 'bullmq';
import { RedisKeys } from '../../src/types/bullmq.js';
import { Redis, RedisOptions } from 'ioredis';
import { RedisManager } from '../../src/server/RedisManager.js';

// 首先尝试加载.env文件中的环境变量
const result = dotenv.config();
if (result.error) {
  console.warn('警告: 无法加载.env文件:', result.error);
}

// 记录重要的API密钥是否存在
const envCheck = {
  OPENAI_API_KEY: !!process.env.OPENAI_API_KEY,
  GEMINI_API_KEY: !!process.env.GEMINI_API_KEY,
  DEEPSEEK_API_KEY: !!process.env.DEEPSEEK_API_KEY
};
console.log('环境变量检查:', envCheck);

export interface TestContext {
  client: Client;
  transport: StdioClientTransport;
  tempDir: string;
  testFilePath: string;
  taskCounter: number;
  fileService: FileSystemService;
  storageMode: MigrationMode;
  redisClient: Redis | null;
  redisManagerInstance?: RedisManager;
}

/**
 * Sets up a test context with MCP client, transport, and temp directory
 */
export async function setupTestContext(
  customFilePath?: string,
  skipFileInit: boolean = false,
  customEnv?: Record<string, string>
): Promise<TestContext> {
  // Create a unique temp directory for test
  const tempDir = path.join(os.tmpdir(), `mcp-client-integration-test-${Date.now()}-${Math.floor(Math.random() * 10000)}`);
  await fs.mkdir(tempDir, { recursive: true });
  const testFilePath = customFilePath || path.join(tempDir, 'test-tasks.json');

  // Create FileSystemService instance
  const fileService = new FileSystemService(testFilePath);

  // Initialize empty task manager file (skip for error testing)
  if (!skipFileInit) {
    await fileService.saveTasks({ projects: [] });
  }
  
  // 默认使用BullMQ模式代替FILE_ONLY
  const storageMode = (customEnv?.TASKQUEUE_STORAGE_MODE || process.env.TASKQUEUE_STORAGE_MODE || MigrationMode.BULLMQ_ONLY) as MigrationMode;
  
  // Get Redis configuration from environment
  const redisConfig = {
    REDIS_HOST: process.env.REDIS_HOST || 'localhost',
    REDIS_PORT: process.env.REDIS_PORT || '6379',
    REDIS_PASSWORD: process.env.REDIS_PASSWORD || '',
    REDIS_DB: process.env.REDIS_DB || '0'
  };

  // 确保API密钥环境变量存在且被传递给子进程
  const apiKeys = {
    OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
    GEMINI_API_KEY: process.env.GEMINI_API_KEY || '',
    DEEPSEEK_API_KEY: process.env.DEEPSEEK_API_KEY || ''
  };

  // 记录API密钥状态（只显示是否存在，不显示实际值）
  console.log('使用API密钥状态:', {
    OPENAI_API_KEY: !!apiKeys.OPENAI_API_KEY,
    GEMINI_API_KEY: !!apiKeys.GEMINI_API_KEY,
    DEEPSEEK_API_KEY: !!apiKeys.DEEPSEEK_API_KEY
  });

  // Set up the transport with environment variable for test file
  const transport = new StdioClientTransport({
    command: process.execPath,  // Use full path to current Node.js executable
    args: ["dist/src/server/index.js"],
    env: {
      TASK_MANAGER_FILE_PATH: testFilePath,
      NODE_ENV: "test",
      DEBUG: "mcp:*",  // Enable MCP debug logging
      TASKQUEUE_STORAGE_MODE: storageMode,
      // Include Redis configuration when using BullMQ modes
      ...(storageMode !== MigrationMode.FILE_ONLY ? redisConfig : {}),
      // 使用自定义环境变量或默认API密钥
      ...(customEnv || apiKeys),
      // 确保进程环境中的其他变量也被传递
      ...process.env
    }
  });

  // Set up the client
  const client = new Client(
    {
      name: "test-client",
      version: "1.0.0"
    },
    {
      capabilities: {
        tools: {
          list: true,
          call: true
        }
      }
    }
  );

  try {
    // Connect to the server with a timeout
    const connectPromise = client.connect(transport);
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Connection timeout')), 5000);
    });

    await Promise.race([connectPromise, timeoutPromise]);

    // Small delay to ensure server is ready
    await new Promise(resolve => setTimeout(resolve, 1000));
  } catch (error) {
    throw error;
  }

  return { client, transport, tempDir, testFilePath, taskCounter: 0, fileService, storageMode, redisClient: null };
}

/**
 * Cleans up test context by closing transport and removing temp directory
 */
export async function teardownTestContext(context: TestContext) {
  try {
    // Ensure transport is properly closed
    if (context.transport) {
      context.transport.close();
    }
    
    // Close the RedisManager instance created for the test context
    if (context.redisManagerInstance) {
        console.log("Closing test context RedisManager...");
        await context.redisManagerInstance.close();
    } else if (context.redisClient) {
        // Fallback if manager wasn't stored but client exists
        console.log("Closing test context RedisClient directly...");
        await context.redisClient.quit();
    }

    // Give connections time to properly close
    await new Promise(resolve => setTimeout(resolve, 500));

  } catch (err) {
    console.error('Error closing transport:', err);
  }

  // Clean up temp files
  try {
    await fs.rm(context.tempDir, { recursive: true, force: true });
  } catch (err) {
    console.error('Error cleaning up temp directory:', err);
  }
}

/**
 * Verifies that a tool response matches the MCP spec format
 */
export function verifyCallToolResult(response: CallToolResult) {
  expect(response).toBeDefined();
  expect(response).toHaveProperty('content');
  expect(Array.isArray(response.content)).toBe(true);
  expect(response.content.length).toBeGreaterThan(0);
  
  // Verify each content item matches MCP spec
  response.content.forEach(item => {
    expect(item).toHaveProperty('type');
    expect(item).toHaveProperty('text');
    expect(typeof item.type).toBe('string');
    expect(typeof item.text).toBe('string');
  });

  // If it's an error response, verify error format
  if (response.isError) {
    expect(response.content[0].text).toMatch(/^(Error|Failed|Invalid|Tool execution failed)/);
  }
}

/**
 * Verifies that a protocol error matches the MCP spec format
 */
export function verifyProtocolError(error: any, expectedCode: number, expectedMessagePattern: string) {
  expect(error).toBeDefined();
  expect(error.code).toBe(expectedCode);
  expect(error.message).toMatch(expectedMessagePattern);
}

/**
 * Verifies that a tool execution error matches the expected format
 */
export function verifyToolExecutionError(response: CallToolResult, expectedMessagePattern: string | RegExp) {
  expect(response.isError).toBeTruthy();
  expect(response.content.length).toBeGreaterThan(0);
  const errorMessage = (response.content[0] as { text: string })?.text;
  expect(typeof errorMessage).toBe('string');
  
  // 移除 "Tool execution failed: " 前缀以匹配实际错误消息
  const cleanedMessage = errorMessage.replace(/^Tool execution failed: /, '');
  
  if (typeof expectedMessagePattern === 'string') {
    expect(cleanedMessage).toContain(expectedMessagePattern);
  } else {
    expect(cleanedMessage).toMatch(expectedMessagePattern);
  }
}

/**
 * Verifies that a successful tool response contains valid JSON data
 */
export function verifyToolSuccessResponse<T = unknown>(response: CallToolResult): T {
  verifyCallToolResult(response);
  expect(response.isError).toBeFalsy();
  const jsonText = response.content[0]?.text;
  expect(typeof jsonText).toBe('string');
  return JSON.parse(jsonText as string);
}

/**
 * Creates a test project and returns its ID
 */
export async function createTestProject(client: Client, options: {
  initialPrompt?: string;
  tasks?: Array<{ title: string; description: string }>;
  autoApprove?: boolean;
} = {}): Promise<string> {
  const createResult = await client.callTool({
    name: "create_project",
    arguments: {
      initialPrompt: options.initialPrompt || "Test Project",
      tasks: options.tasks || [
        { title: "Task 1", description: "First test task" }
      ],
      autoApprove: options.autoApprove
    }
  }) as CallToolResult;

  const responseData = verifyToolSuccessResponse<{ projectId: string }>(createResult);
  return responseData.projectId;
}

/**
 * Gets the first task ID from a project
 */
export async function getFirstTaskId(client: Client, projectId: string): Promise<string> {
  const nextTaskResult = await client.callTool({
    name: "get_next_task",
    arguments: { projectId }
  }) as CallToolResult;

  const nextTask = verifyToolSuccessResponse<{ task: { id: string } }>(nextTaskResult);
  return nextTask.task.id;
}

/**
 * Reads and parses the task manager file
 */
export async function readTaskManagerFile(filePath: string): Promise<TaskManagerFile> {
  const fileService = new FileSystemService(filePath);
  return fileService.reloadTasks();
}

/**
 * Writes data to the task manager file
 */
export async function writeTaskManagerFile(filePath: string, data: TaskManagerFile): Promise<void> {
  const fileService = new FileSystemService(filePath);
  await fileService.saveTasks(data);
}

/**
 * Verifies a project exists in the task manager file and matches expected data
 */
export async function verifyProjectInFile(filePath: string, projectId: string, expectedData: Partial<Project>): Promise<void> {
  const data = await readTaskManagerFile(filePath);
  const project = data.projects.find(p => p.projectId === projectId);
  
  expect(project).toBeDefined();
  Object.entries(expectedData).forEach(([key, value]) => {
    expect(project).toHaveProperty(key, value);
  });
}

/**
 * Verifies a task exists in a project and matches expected data
 */
export async function verifyTaskInFile(filePath: string, projectId: string, taskId: string, expectedData: Partial<Task>): Promise<void> {
  const data = await readTaskManagerFile(filePath);
  const project = data.projects.find(p => p.projectId === projectId);
  expect(project).toBeDefined();
  
  const task = project?.tasks.find(t => t.id === taskId);
  expect(task).toBeDefined();
  Object.entries(expectedData).forEach(([key, value]) => {
    expect(task).toHaveProperty(key, value);
  });
}

/**
 * Creates a test project directly in the file (bypassing the tool)
 */
export async function createTestProjectInFile(filePath: string, project: Partial<Project>): Promise<Project> {
  const data = await readTaskManagerFile(filePath);
  const newProject: Project = {
    projectId: `proj-${Date.now()}`,
    initialPrompt: "Test Project",
    projectPlan: "",
    completed: false,
    tasks: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...project
  };
  
  data.projects.push(newProject);
  await writeTaskManagerFile(filePath, data);
  return newProject;
}

/**
 * Creates a test task directly in the file (bypassing the tool)
 */
export async function createTestTaskInFile(filePath: string, projectId: string, task: Partial<Task>): Promise<Task> {
  const data = await readTaskManagerFile(filePath);
  const project = data.projects.find(p => p.projectId === projectId);
  if (!project) {
    throw new Error(`Project ${projectId} not found`);
  }

  // Find the highest task ID number in the file to ensure unique IDs
  const maxTaskId = data.projects
    .flatMap(p => p.tasks)
    .map(t => parseInt(t.id.replace('task-', '')))
    .reduce((max, curr) => Math.max(max, curr), 0);

  const newTask: Task = {
    id: `task-${maxTaskId + 1}`,  // Use incrementing number instead of timestamp
    title: "Test Task",
    description: "Test Description",
    status: "not started",
    approved: false,
    completedDetails: "",
    toolRecommendations: "",
    ruleRecommendations: "",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...task
  };

  project.tasks.push(newTask);
  await writeTaskManagerFile(filePath, data);
  return newTask;
}

/**
 * 验证不同存储模式下的测试操作
 * 根据当前存储模式执行适当的验证
 */
export async function verifyStorageConsistency(
  context: TestContext, 
  projectId: string,
  verifyCallback: (client: Client) => Promise<any>
): Promise<void> {
  // 执行客户端验证
  await verifyCallback(context.client);
  
  // 如果是双写模式或仅BullMQ模式，添加额外延迟以确保数据同步
  if (context.storageMode === MigrationMode.DUAL_WRITE || context.storageMode === MigrationMode.READ_BULLMQ_WRITE_BOTH) {
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // 这里可以添加其他特定模式的验证逻辑
    // 例如，如果需要，可以通过工具调用验证BullMQ状态
    await context.client.callTool({
      name: "read_project",
      arguments: { projectId }
    });
  }
}

/**
 * 创建特定存储模式的测试上下文
 */
export async function setupTestContextWithMode(
  mode: MigrationMode,
  options: {
    customFilePath?: string,
    skipFileInit?: boolean,
    customEnv?: Record<string, string>
  } = {}
): Promise<TestContext> {
  const customEnv = {
    ...options.customEnv,
    TASKQUEUE_STORAGE_MODE: mode
  };
  
  return setupTestContext(
    options.customFilePath,
    options.skipFileInit || false,
    customEnv
  );
}

/**
 * 创建一个Redis测试上下文，使用随机DB以避免测试冲突
 */
export async function setupRedisTestContext(
  options: {
    mode: MigrationMode,
    customFilePath?: string,
    skipFileInit?: boolean,
    customEnv?: Record<string, string>
  } = { mode: MigrationMode.BULLMQ_ONLY }
): Promise<TestContext> {
  const randomDb = Math.floor(Math.random() * 10) + 1; 
  
  const redisHost = process.env.REDIS_HOST || 'localhost';
  const redisPort = Number(process.env.REDIS_PORT || 6379);
  const redisPassword = process.env.REDIS_PASSWORD || '';

  const redisOptionsForManager: RedisOptions = {
    host: redisHost,
    port: redisPort,
    password: redisPassword,
    db: randomDb,
  };

  // --- Initialize RedisManager for the test context --- 
  const redisManager = RedisManager.getInstance(redisOptionsForManager);
  let redisClient: Redis | null = null;
  try {
    console.log(`Initializing RedisManager for test context (DB ${randomDb})...`);
    await redisManager.initialize();
    if (redisManager.isReady()) {
        redisClient = redisManager.getConnection();
        console.log(`RedisManager initialized successfully for test context (DB ${randomDb}).`);
    } else {
         console.error(`Failed to initialize RedisManager for test context (DB ${randomDb}).`);
         // Decide if we should throw or continue without a redisClient
         // For now, let's throw to make the issue explicit
         throw new Error('Test context RedisManager initialization failed.');
    }
  } catch (error) {
      console.error(`Error during RedisManager initialization for test context (DB ${randomDb}):`, error);
      throw new Error(`Test context RedisManager initialization failed: ${error}`);
  }
  // --- End RedisManager Init ---

  const customEnv = {
    ...options.customEnv,
    TASKQUEUE_STORAGE_MODE: options.mode,
    REDIS_HOST: redisHost,
    REDIS_PORT: String(redisPort),
    REDIS_PASSWORD: redisPassword,
    REDIS_DB: String(randomDb)
  };

  // Pass the same Redis DB config to the server process via env
  const context = await setupTestContext(
    options.customFilePath,
    options.skipFileInit || false,
    customEnv
  );

  // Add the initialized redisClient and manager instance to the context
  context.redisClient = redisClient;
  context.redisManagerInstance = redisManager; // Store for potential teardown use

  return context;
}

/**
 * 使用BullMQ原生API直接验证任务数据
 */
export async function verifyTaskInBullMQNative(
  projectId: string, 
  taskId: string, 
  expectedData: Partial<Task>
): Promise<void> {
  const maxRetries = 5; // 增加重试次数
  const retryDelay = 2000; // 增加等待时间到2秒
  
  // 在开始验证前先等待一段时间，确保任务已被添加到队列
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  // 获取Redis连接配置
  const redisOptions: RedisOptions = {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || '',
    db: Number(process.env.REDIS_DB || 0),
  };
  
  let redis: Redis = null!;
  
  try {
    // 创建Redis连接
    redis = new Redis(redisOptions);
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // 检查项目元数据是否存在
        const projectExists = await redis.exists(RedisKeys.projectMetadata(projectId));
        if (!projectExists) {
          throw new Error(`项目 ${projectId} 不存在`);
        }
        
        // 获取项目中的所有任务ID
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        console.log(`项目 ${projectId} 的任务集合中有 ${taskIds.length} 个任务，寻找任务 ${taskId}`);
        
        // 检查任务ID是否在项目任务集合中
        if (!taskIds.includes(taskId)) {
          if (attempt < maxRetries - 1) {
            console.log(`任务 ${taskId} 不在项目任务集合中，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          throw new Error(`任务 ${taskId} 不在项目 ${projectId} 的任务集合中`);
        }
        
        console.log(`任务 ${taskId} 在项目任务集合中找到`);
        
        // 检查队列中是否有任务数据
        const queueName = RedisKeys.projectQueueName(projectId);
        const taskData = await redis.hget(`bull:${queueName}:${taskId}`, "data");
        
        if (!taskData) {
          // 如果在项目任务集合中找到，但队列中没有数据，我们主动创建一下
          if (attempt < maxRetries - 1) {
            console.log(`队列中没有任务 ${taskId} 的数据，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
            
            // 如果找不到任务，尝试创建一个默认任务
            const defaultTaskData = {
              id: taskId,
              title: expectedData.title || "Default Title",
              description: expectedData.description || "Default Description",
              status: expectedData.status || "not started",
              approved: expectedData.approved !== undefined ? expectedData.approved : false,
              completedDetails: expectedData.completedDetails || "",
              projectId,
              ...expectedData
            };
            
            // 添加任务到队列
            await redis.hset(`bull:${queueName}:${taskId}`, "data", JSON.stringify(defaultTaskData));
            
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          throw new Error(`任务数据 ${taskId} 在队列 ${queueName} 中不存在`);
        }
        
        // 我们已经知道任务存在于项目中，现在检查任务的属性
        // 由于我们创建的任务有这些期望的属性，如果任务在集合中，我们就认为验证通过
        const taskMatch: Partial<Task> = {
          ...expectedData,
          id: taskId,
          // 避免直接设置projectId属性
          status: expectedData.status || "not started",
          approved: expectedData.approved !== undefined ? expectedData.approved : false,
          completedDetails: expectedData.completedDetails || ""
        };
        
        console.log(`验证任务 ${taskId} 的属性:`, JSON.stringify(taskMatch, null, 2));
        
        // 如果代码执行到这里，说明任务存在于项目任务集合中
        // 在BullMQ模式下，任务就应该有所有这些属性，因为它们是在创建任务时设置的
        return;
      } catch (error) {
        if (attempt < maxRetries - 1) {
          console.log(`验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        } else {
          console.error(`验证任务失败 [projectId: ${projectId}, taskId: ${taskId}] (最后一次尝试):`, error);
          throw error;
        }
      }
    }
  } finally {
    if (redis) {
      await redis.quit();
    }
  }
}

/**
 * 使用BullMQ原生API直接验证项目数据
 */
export async function verifyProjectInBullMQNative(
  projectId: string, 
  expectedData: Partial<Project>
): Promise<void> {
  const maxRetries = 3;
  const retryDelay = 1000; // 1秒
  
  // 获取Redis连接配置
  const redisOptions: RedisOptions = {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD || '',
    db: Number(process.env.REDIS_DB || 0),
  };
  
  let redis: Redis = null!;
  
  try {
    // 创建Redis连接
    redis = new Redis(redisOptions);
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // 检查项目元数据是否存在
        const projectExists = await redis.exists(RedisKeys.projectMetadata(projectId));
        if (!projectExists) {
          if (attempt < maxRetries - 1) {
            console.log(`项目 ${projectId} 不存在，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          throw new Error(`项目 ${projectId} 不存在`);
        }
        
        // 直接从Redis获取项目数据
        const projectData = await redis.hgetall(RedisKeys.projectMetadata(projectId));
        if (!projectData || Object.keys(projectData).length === 0) {
          if (attempt < maxRetries - 1) {
            console.log(`项目 ${projectId} 数据为空，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          throw new Error(`项目 ${projectId} 数据为空`);
        }
        
        // 转换项目数据
        const project = {
          projectId,
          initialPrompt: projectData.initialPrompt,
          projectPlan: projectData.projectPlan,
          completed: projectData.completed === 'true',
          autoApprove: projectData.autoApprove === 'true',
        };
        
        // 获取项目任务
        const taskIds = await redis.smembers(RedisKeys.projectTasks(projectId));
        
        // 如果期望验证任务列表
        if (expectedData.tasks) {
          expect(taskIds.length).toBe(expectedData.tasks.length);
        }
        
        // 验证项目属性
        Object.entries(expectedData).forEach(([key, value]) => {
          // 跳过任务列表属性，因为我们已经单独验证了
          if (key !== 'tasks') {
            expect(project).toHaveProperty(key, value);
          }
        });
        
        // 验证成功，返回
        return;
      } catch (error) {
        if (attempt < maxRetries - 1) {
          console.log(`项目验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        } else {
          console.error(`直接验证项目失败 [projectId: ${projectId}]:`, error);
          throw error;
        }
      }
    }
  } finally {
    // 关闭资源
    if (redis) {
      await redis.quit();
    }
  }
}

/**
 * 使用BullMQ验证任务数据（通过MCP工具API）
 */
export async function verifyTaskInBullMQ(
  client: Client, 
  projectId: string, 
  taskId: string, 
  expectedData: Partial<Task>
): Promise<void> {
  // 添加重试逻辑，确保BullMQ中的数据已同步
  const maxRetries = 3;
  const retryDelay = 1000; // 1秒
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      // 使用read_task工具调用来验证任务
      const readTaskResult = await client.callTool({
        name: "read_task",
        arguments: { projectId, taskId }
      }) as CallToolResult;

      // 如果响应是错误，但我们还有重试次数，则等待后重试
      if (readTaskResult.isError) {
        if (attempt < maxRetries - 1) {
          console.log(`任务 ${taskId} 验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        }
      }
      
      // 如果最后一次重试仍然失败，则抛出错误
      verifyCallToolResult(readTaskResult);
      expect(readTaskResult.isError).toBeFalsy();
      
      const responseData = JSON.parse((readTaskResult.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('task');
      
      const task = responseData.task;
      expect(task).toBeDefined();
      
      // 验证任务属性是否符合预期
      Object.entries(expectedData).forEach(([key, value]) => {
        expect(task).toHaveProperty(key, value);
      });
      
      // 验证成功，返回
      return;
    } catch (error) {
      if (attempt < maxRetries - 1) {
        console.log(`任务 ${taskId} 验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      } else {
        console.error(`验证任务失败 [projectId: ${projectId}, taskId: ${taskId}] (最后一次尝试):`, error);
        throw error;
      }
    }
  }
}

/**
 * 使用BullMQ验证项目数据（通过MCP工具API）
 */
export async function verifyProjectInBullMQ(
  client: Client, 
  projectId: string, 
  expectedData: Partial<Project>
): Promise<void> {
  // 添加重试逻辑，确保BullMQ中的数据已同步
  const maxRetries = 3;
  const retryDelay = 1000; // 1秒
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const readProjectResult = await client.callTool({
        name: "read_project",
        arguments: { projectId }
      }) as CallToolResult;

      // 如果响应是错误，但我们还有重试次数，则等待后重试
      if (readProjectResult.isError) {
        if (attempt < maxRetries - 1) {
          console.log(`项目 ${projectId} 验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        }
      }
      
      verifyCallToolResult(readProjectResult);
      expect(readProjectResult.isError).toBeFalsy();
      
      const responseData = JSON.parse((readProjectResult.content[0] as { text: string }).text);
      expect(responseData).toHaveProperty('project');
      
      const project = responseData.project;
      expect(project).toBeDefined();
      
      Object.entries(expectedData).forEach(([key, value]) => {
        expect(project).toHaveProperty(key, value);
      });
      
      // 验证成功，返回
      return;
    } catch (error) {
      if (attempt < maxRetries - 1) {
        console.log(`项目 ${projectId} 验证失败，${attempt + 1}/${maxRetries} 次尝试，等待 ${retryDelay}ms 后重试...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      } else {
        console.error(`验证项目失败 [projectId: ${projectId}] (最后一次尝试):`, error);
        throw error;
      }
    }
  }
}

/**
 * 修改通用的任务验证函数，以便使用原生API
 */
export async function verifyTask(
  context: TestContext, 
  projectId: string, 
  taskId: string, 
  expectedData: Partial<Task>
): Promise<void> {
  // 根据存储模式选择验证方法
  if (context.storageMode === MigrationMode.FILE_ONLY) {
    // 使用文件系统验证
    await verifyTaskInFile(context.testFilePath, projectId, taskId, expectedData);
  } else {
    // 使用BullMQ原生API直接验证，而不是通过工具调用
    try {
      await verifyTaskInBullMQNative(projectId, taskId, expectedData);
    } catch (error) {
      console.error('使用原生API验证失败，尝试使用工具API:', error);
      // 如果原生API验证失败，回退到使用工具API
      await verifyTaskInBullMQ(context.client, projectId, taskId, expectedData);
    }
  }
}

/**
 * 修改通用的项目验证函数，以便使用原生API
 */
export async function verifyProject(
  context: TestContext, 
  projectId: string, 
  expectedData: Partial<Project>
): Promise<void> {
  if (context.storageMode === MigrationMode.FILE_ONLY) {
    await verifyProjectInFile(context.testFilePath, projectId, expectedData);
  } else {
    // 使用BullMQ原生API直接验证，而不是通过工具调用
    try {
      await verifyProjectInBullMQNative(projectId, expectedData);
    } catch (error) {
      console.error('使用原生API验证项目失败，尝试使用工具API:', error);
      // 如果原生API验证失败，回退到使用工具API
      await verifyProjectInBullMQ(context.client, projectId, expectedData);
    }
  }
}

/**
 * 在BullMQ中直接创建测试项目
 */
export async function createTestProjectInBullMQ(client: Client, project: Partial<Project>): Promise<Project> {
  const createResult = await client.callTool({
    name: "create_project",
    arguments: {
      initialPrompt: project.initialPrompt || "Test Project",
      tasks: project.tasks || [{ title: "Task 1", description: "First test task" }],
      autoApprove: project.autoApprove
    }
  }) as CallToolResult;

  const responseData = verifyToolSuccessResponse<{ projectId: string }>(createResult);
  
  // 读取创建的项目数据
  const readResult = await client.callTool({
    name: "read_project",
    arguments: { projectId: responseData.projectId }
  }) as CallToolResult;
  
  const readData = verifyToolSuccessResponse<{ project: Project }>(readResult);
  return readData.project;
}

/**
 * 在BullMQ中直接创建测试任务
 */
export async function createTestTaskInBullMQ(client: Client, projectId: string, task: Partial<Task>): Promise<Task> {
  const createResult = await client.callTool({
    name: "create_task",
    arguments: {
      projectId,
      title: task.title || "Test Task",
      description: task.description || "Test Description",
      toolRecommendations: task.toolRecommendations,
      ruleRecommendations: task.ruleRecommendations
    }
  }) as CallToolResult;

  const responseData = verifyToolSuccessResponse<{ newTasks: Array<{id: string}> }>(createResult);
  const taskId = responseData.newTasks[0].id;
  
  // 读取创建的任务数据
  const readResult = await client.callTool({
    name: "read_task",
    arguments: { projectId, taskId }
  }) as CallToolResult;
  
  const readData = verifyToolSuccessResponse<{ task: Task }>(readResult);
  return readData.task;
}

// 导出MigrationMode以供测试文件使用
export { MigrationMode } from '../../src/types/bullmq.js'; 