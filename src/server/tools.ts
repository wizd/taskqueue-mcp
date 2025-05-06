import { Tool } from "@modelcontextprotocol/sdk/types.js";
import { TaskManager } from "./TaskManager.js";
import { TaskManagerBase } from "./TaskManagerBase.js";
import { toolExecutorMap } from "./toolExecutors.js";
import { AppError, AppErrorCode } from "../types/errors.js";
import { McpError, CallToolResult, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { UpdateTaskSuccessData } from '../types/response.js';

// ---------------------- PROJECT TOOLS ----------------------

/**
 * List Projects Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {listProjectsToolExecutor}
 */
const listProjectsTool: Tool = {
  name: "list_projects",
  description: "列出系统中所有项目及其基本信息（ID、初始提示、任务数量），可选按状态过滤（open、pending_approval、completed、all）。基于Redis分布式队列，支持高并发查询。",
  inputSchema: {
    type: "object",
    properties: {
      state: {
        type: "string",
        enum: ["open", "pending_approval", "completed", "all"],
        description: "按状态筛选项目。'open'（任何未完成任务）、'pending_approval'（任何等待审批的任务）、'completed'（所有任务已完成并已审批）或'all'跳过筛选。",
      },
    },
    required: [],
  },
};

/**
 * Read Project Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {readProjectToolExecutor}
 */
const readProjectTool: Tool = {
  name: "read_project",
  description: "通过ID读取项目的所有信息，包括任务状态。使用Redis作为后端存储，实现毫秒级响应时间和高可靠性。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要读取的项目ID（例如，proj-1）。",
      },
    },
    required: ["projectId"],
  },
};

/**
 * Create Project Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {createProjectToolExecutor}
 */
const createProjectTool: Tool = {
  name: "create_project",
  description: "创建一个新项目，包含初始提示和任务列表。任务将作为分布式队列中的作业进行处理，支持优先级、延迟执行和自动重试机制。这通常是任何工作流程的第一步。",
  inputSchema: {
    type: "object",
    properties: {
      initialPrompt: {
        type: "string",
        description: "项目的初始提示或目标。",
      },
      projectPlan: {
        type: "string",
        description: "项目的更详细计划。如果未提供，将使用初始提示。",
      },
      tasks: {
        type: "array",
        description: "任务对象数组，每个任务将作为独立作业加入队列进行处理。",
        items: {
          type: "object",
          properties: {
            title: {
              type: "string",
              description: "任务的标题。",
            },
            description: {
              type: "string",
              description: "任务的详细描述。",
            },
            toolRecommendations: {
              type: "string",
              description: "完成任务建议使用的工具。",
            },
            ruleRecommendations: {
              type: "string",
              description: "完成任务时建议查看的相关规则。",
            },
          },
          required: ["title", "description"],
        },
      },
      autoApprove: {
        type: "boolean",
        description: "如果为true，任务标记为完成时将自动审批。如果为false或未提供，任务需要手动审批。",
      },
    },
    required: ["initialPrompt", "tasks"],
  },
};

/**
 * Delete Project Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {deleteProjectToolExecutor}
 */
const deleteProjectTool: Tool = {
  name: "delete_project",
  description: "删除项目及其所有关联任务。系统将自动清理队列、移除Redis中的任务数据，并释放相关资源。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要删除的项目ID（例如，proj-1）。",
      },
    },
    required: ["projectId"],
  },
};

/**
 * Add Tasks to Project Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {addTasksToProjectToolExecutor}
 */
const addTasksToProjectTool: Tool = {
  name: "add_tasks_to_project",
  description: "向现有项目添加新任务。任务将作为BullMQ作业添加到Redis队列中，支持FIFO/LIFO处理顺序，以及任务优先级设置。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要添加任务的项目ID（例如，proj-1）。",
      },
      tasks: {
        type: "array",
        description: "要添加的任务对象数组。",
        items: {
          type: "object",
          properties: {
            title: {
              type: "string",
              description: "任务的标题。",
            },
            description: {
              type: "string",
              description: "任务的详细描述。",
            },
            toolRecommendations: {
              type: "string",
              description: "完成任务建议使用的工具。",
            },
            ruleRecommendations: {
              type: "string",
              description: "完成任务时建议查看的相关规则。",
            },
          },
          required: ["title", "description"],
        },
      },
    },
    required: ["projectId", "tasks"],
  },
};

/**
 * Finalize Project Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {finalizeProjectToolExecutor}
 */
const finalizeProjectTool: Tool = {
  name: "finalize_project",
  description: "将项目标记为完成。只有当所有任务都已完成并审批时才能调用。系统将验证所有任务的状态，确保完整性后更新项目元数据。这通常是项目工作流程的最后一步。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要完成的项目ID（例如，proj-1）。",
      },
    },
    required: ["projectId"],
  },
};

/**
 * Generate Project Plan Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {generateProjectPlanToolExecutor}
 */
const generateProjectPlanTool: Tool = {
  name: "generate_project_plan",
  description: "使用LLM从提示生成项目计划和任务。LLM将分析提示和任何附加文件以创建结构化项目计划，任务将自动添加到分布式任务队列中进行后续处理。",
  inputSchema: {
    type: "object",
    properties: {
      prompt: {
        type: "string",
        description: "用于生成项目计划的提示文本或文件路径。",
      },
      provider: {
        type: "string",
        enum: ["openai", "google", "deepseek"],
        description: "要使用的LLM提供者（需要设置相应的API密钥）。",
      },
      model: {
        type: "string",
        description: "要使用的特定模型（例如，OpenAI的'gpt-4-turbo'）。",
      },
      attachments: {
        type: "array",
        items: {
          type: "string",
        },
        description: "可选的作为上下文附加的文件路径数组。调用此工具前无需阅读这些文件！",
      },
    },
    required: ["prompt", "provider", "model"],
  },
};

// ---------------------- TASK TOOLS ----------------------

/**
 * List Tasks Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {listTasksToolExecutor}
 */
const listTasksTool: Tool = {
  name: "list_tasks",
  description: "列出所有任务，可选按项目ID和/或状态（open、pending_approval、completed、all）过滤。支持实时状态查询，利用Redis高性能索引快速检索任务数据。任务可能包含指导其完成的工具和规则建议。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要列出任务的项目ID。如果省略，列出所有任务。",
      },
      state: {
        type: "string",
        enum: ["open", "pending_approval", "completed", "all"],
        description: "按状态筛选任务。'open'（未开始/进行中），'pending_approval'，'completed'，或'all'跳过筛选。",
      },
    },
    required: [], // Neither projectId nor state is required, both are optional filters
  },
};

/**
 * Read Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {readTaskToolExecutor}
 */
const readTaskTool: Tool = {
  name: "read_task",
  description: "通过ID获取特定任务的详细信息。任务数据存储在Redis中，支持高速访问和可靠性。任务可能包含toolRecommendations和ruleRecommendations字段，应用于指导任务完成。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "包含任务的项目ID（例如，proj-1）。",
      },
      taskId: {
        type: "string",
        description: "要读取的任务ID（例如，task-1）。",
      },
    },
    required: ["projectId", "taskId"],
  },
};

/**
 * Create Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {createTaskToolExecutor}
 */
const createTaskTool: Tool = {
  name: "create_task",
  description: "在现有项目中创建新任务。任务将作为BullMQ作业存储在Redis中，支持自动重试、延迟执行和优先级设置。您可以选择包含工具和规则建议以指导任务完成。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要添加任务的项目ID（例如，proj-1）。",
      },
      title: {
        type: "string",
        description: "任务的标题。",
      },
      description: {
        type: "string",
        description: "任务的详细描述。",
      },
      toolRecommendations: {
        type: "string",
        description: "完成任务建议使用的工具。",
      },
      ruleRecommendations: {
        type: "string",
        description: "完成任务时建议查看的相关规则。",
      }
    },
    required: ["projectId", "title", "description"]
  }
};

/**
 * Update Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {updateTaskToolExecutor}
 */
const updateTaskTool: Tool = {
  name: "update_task",
  description: "修改任务属性。系统支持实时更新，任务状态变更会触发相应的事件通知。注意：(1)设置状态为'done'时需要completedDetails，(2)已审批的任务无法修改，(3)状态必须遵循有效转换：not started → in progress → done。您还可以更新工具和规则建议以指导任务完成。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "包含任务的项目ID（例如，proj-1）。",
      },
      taskId: {
        type: "string",
        description: "要更新的任务ID（例如，task-1）。",
      },
      title: {
        type: "string",
        description: "任务的新标题（可选）。",
      },
      description: {
        type: "string",
        description: "任务的新描述（可选）。",
      },
      status: {
        type: "string",
        enum: ["not started", "in progress", "done"],
        description: "任务的新状态（可选）。",
      },
      completedDetails: {
        type: "string",
        description: "任务完成的详细信息（如果状态设置为'done'则必需）。",
      },
      toolRecommendations: {
        type: "string",
        description: "完成任务建议使用的工具。",
      },
      ruleRecommendations: {
        type: "string",
        description: "完成任务时建议查看的相关规则。",
      }
    },
    required: ["projectId", "taskId"], // title, description, status are optional, but completedDetails is conditionally required
  },
};

/**
 * Delete Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {deleteTaskToolExecutor}
 */
const deleteTaskTool: Tool = {
  name: "delete_task",
  description: "从项目中移除任务。操作将从Redis队列中删除任务作业，并清除相关元数据，支持原子性操作以确保数据一致性。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "包含任务的项目ID（例如，proj-1）。",
      },
      taskId: {
        type: "string",
        description: "要删除的任务ID（例如，task-1）。",
      },
    },
    required: ["projectId", "taskId"],
  },
};

/**
 * Approve Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {approveTaskToolExecutor}
 */
const approveTaskTool: Tool = {
  name: "approve_task",
  description: "审批已完成的任务。任务必须标记为'done'并提供completedDetails才能审批。系统利用BullMQ事件机制追踪任务状态变更，并支持自动化工作流程。注意：这是仅限CLI的操作，需要人工干预。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "包含任务的项目ID（例如，proj-1）。",
      },
      taskId: {
        type: "string",
        description: "要审批的任务ID（例如，task-1）。",
      }
    },
    required: ["projectId", "taskId"]
  }
};

/**
 * Get Next Task Tool
 * @param {object} args - A JSON object containing the arguments
 * @see {getNextTaskToolExecutor}
 */
const getNextTaskTool: Tool = {
  name: "get_next_task",
  description: "获取项目中下一个要完成的任务。系统基于BullMQ队列的FIFO原则返回序列中第一个未审批的任务，无论其状态如何。利用Redis高性能查询，确保毫秒级响应时间。任务可能包含toolRecommendations和ruleRecommendations字段，应用于指导任务完成。",
  inputSchema: {
    type: "object",
    properties: {
      projectId: {
        type: "string",
        description: "要获取下一个任务的项目ID（例如，proj-1）。",
      },
    },
    required: ["projectId"],
  },
};

// Export all tools as an array
export const ALL_TOOLS: Tool[] = [
  listProjectsTool,
  readProjectTool,
  createProjectTool,
  deleteProjectTool,
  addTasksToProjectTool,
  finalizeProjectTool,
  generateProjectPlanTool,

  listTasksTool,
  readTaskTool,
  createTaskTool,
  updateTaskTool,
  deleteTaskTool,
  approveTaskTool,
  getNextTaskTool,
];

/**
 * Finds and executes a tool, handling error classification.
 * - Throws errors tagged with `jsonRpcCode` for protocol issues (e.g., Not Found, Invalid Params).
 * - Catches other errors (tool execution failures) and returns the standard MCP error result format.
 */
export async function executeToolAndHandleErrors(
  toolName: string,
  args: Record<string, unknown>,
  taskManager: TaskManagerBase
): Promise<CallToolResult> {
  const executor = toolExecutorMap.get(toolName);

  // 1. Handle "Tool Not Found"
  if (!executor) {
    const protocolError = new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${toolName}`);
    throw protocolError; // Throw McpError for SDK to handle
  }

  try {
    // 从参数中提取租户ID（如果存在）并进行处理
    const tenantId = args._tenantId as string | undefined;
    
    // 如果租户ID包含前导冒号，记录警告并修正
    if (tenantId && typeof tenantId === 'string' && tenantId.startsWith(':')) {
      console.warn(`检测到错误的租户ID格式: ${tenantId}，自动修正`);
    }
    
    // 从参数中移除租户ID，防止它被传递给工具执行器
    delete args._tenantId;
    
    // 2. Execute the tool - Validation errors (protocol) or TaskManager errors (execution) might be thrown
    const resultData = await executor.execute(taskManager, args);

    // 3. Format successful execution result using standard stringify
    const responseText = JSON.stringify(resultData, null, 2);

    return {
      content: [{ type: "text", text: responseText }]
    };

  } catch (error: AppError | unknown) {
    // 4a. Handle protocol errors (missing params, invalid args)
    if (error instanceof AppError) {
      if ([
          AppErrorCode.MissingParameter, 
          AppErrorCode.InvalidArgument
        ].includes(error.code as AppErrorCode)
      ) {
        throw new McpError(ErrorCode.InvalidParams, error.message);
      }
    }

    // 4b. Handle all other errors as tool execution failures
    console.error(`Tool Execution Error [${toolName}]:`, error);

    // Get error message, handling both Error objects and unknown error types
    const errorMessage = error instanceof Error 
      ? error.message 
      : String(error);

    // Format and RETURN the error within the 'result' field structure.
    const result: CallToolResult = {
      content: [{ type: "text", text: `Tool execution failed: ${errorMessage}` }],
      isError: true // Mark as an execution error as per MCP spec
    };
    
    return result;
  }
}