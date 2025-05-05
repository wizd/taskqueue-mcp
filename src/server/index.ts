#!/usr/bin/env node

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { RestServerTransport } from "@chatmcp/sdk/server/rest.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { TaskManagerFactory } from "./TaskManagerFactory.js";
import { ALL_TOOLS, executeToolAndHandleErrors } from "./tools.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { getParamValue, getAuthValue } from "@chatmcp/sdk/utils/index.js";
import { MigrationMode } from "../types/bullmq.js";
import dotenv from 'dotenv';

// 加载环境变量
dotenv.config();

// Create server with capabilities BEFORE setting up handlers
const server = new Server(
  {
    name: "task-manager-server",
    version: "1.4.1"
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

// 显式设置使用BullMQ模式
const taskManager = TaskManagerFactory.createTaskManager(MigrationMode.BULLMQ_ONLY);

const mode = getParamValue("MODE") || "stdio";
const port = getParamValue("PORT") || 9593;
const endpoint = getParamValue("ENDPOINT") || "/rest";

// Set up request handlers AFTER capabilities are configured
server.setRequestHandler(ListToolsRequestSchema, async (request) => {
  // 获取租户ID
  const tenantId = request.params?._tenantId as string | undefined;
  console.error(`处理来自租户 ${tenantId || 'default'} 的工具列表请求`);
  
  // 这里可以基于租户ID返回不同的工具列表
  // 目前返回所有工具，但未来可以基于租户进行筛选
  return {
    tools: ALL_TOOLS
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  // 获取租户ID
  const tenantId = request.params?._tenantId as string | undefined;
  console.error(`处理来自租户 ${tenantId || 'default'} 的工具调用请求: ${request.params?.name}`);
  
  // 检查租户是否有权限访问请求的工具
  if (!hasPermission(tenantId, request.params?.name as string)) {
    throw new Error(`租户 ${tenantId || 'default'} 无权访问工具 ${request.params?.name}`);
  }
  
  // 创建带有租户ID的任务管理器
  const tenantTaskManager = tenantId 
    ? TaskManagerFactory.createTaskManagerWithTenant(MigrationMode.BULLMQ_ONLY, tenantId)
    : taskManager;
  
  // Directly call the handler. It either returns a result object (success or isError:true)
  // OR it throws a tagged protocol error.
  return await executeToolAndHandleErrors(
    request.params!.name,
    {
      ...request.params?.arguments || {},
      _tenantId: tenantId  // 将租户ID传递给工具执行上下文
    },
    tenantTaskManager
  );
  // SDK automatically handles:
  // - Wrapping the returned value (success data or isError:true object) in `result: { ... }`
  // - Catching re-thrown protocol errors and formatting the top-level `error: { ... }`
});

// 简单的权限检查函数，未来可以扩展为更复杂的实现
function hasPermission(tenantId: string | undefined, toolName: string): boolean {
  // 实现基本权限检查逻辑
  // 目前允许所有访问，但这里可以实现特定的权限控制
  return true;
}

// 启动服务器
async function runServer() {
  try {
    // 根据模式选择传输方式
    if (mode === "rest") {
      const transport = new RestServerTransport({
        port,
        endpoint,
        supportTenantId: true  // 启用多租户支持
      });
      await server.connect(transport);
      
      await transport.startServer();
      
      console.error(
        `Task Manager MCP Server running on REST with port ${port} and endpoint ${endpoint}`
      );
      return;
    }
    
    // 使用stdio传输方式
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error(
      "Task Manager MCP Server running on stdio"
    );
  } catch (error) {
    console.error("启动服务器时发生致命错误:", error);
    process.exit(1);
  }
}

// 运行服务器
runServer();
