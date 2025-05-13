import {
  generateText,
  tool,
  CoreMessage,
  ToolCallPart,
  ToolResultPart,
  Tool,
  ToolSet,
  GenerateTextResult,
} from "@wizdy/ai";  //this is a fork of Vercel AI SDK, use the official doc is OK.
import { createMCPClient } from '../../lib/mcp/streamHttpClient.js';
import { BullMQTaskData } from "../../src/types/bullmq.js";
import { Logger } from '../../src/server/Logger.js';
import { Project } from "../../src/types/data.js";
import { modelProvider } from "../ai/provider.js";
import { z } from "zod"; // Import Zod if needed for tool schemas, or adjust based on actual tool definitions

// Define a type for MCP client tools if possible, otherwise use \'any\' cautiously
// type McpTool = (args: any) => Promise<any>; // Removed
// type ToolMap = Record<string, McpTool>; // Removed

export async function runTask(
  taskData: BullMQTaskData,
  projectId: string,
  job: any,
  logger: Logger,
  readProjectFunction?: (
    projectId: string
  ) => Promise<Project>,
  finalizeProjectFunction?: (
    projectId: string,
    conclusion: string
  ) => Promise<void>
) {
  logger.info(
    `开始处理任务 ${taskData.id} (${taskData.title}) (项目: ${projectId})`
  );
  await job.updateProgress(10);

  let mcpClientsToClose: Awaited<
    ReturnType<typeof createMCPClient> // Assuming createMCPClient returns a specific type with close method
  >[] = [];
  let tools: ToolSet = {}; // Use ToolSet type

  // Store client info for logging
  const clientInfo: { prefix: string; client: Awaited<ReturnType<typeof createMCPClient>> }[] = [];

  try {
    // --- Setup MCP Clients ---
    const ytdlp_url = process.env.YTDLP_MCP_URL;
    const ytdlp_api_key = process.env.YTDLP_MCP_API_KEY;
    if (!ytdlp_url || !ytdlp_api_key) {
      throw new Error("YTDLP_MCP_URL 或 YTDLP_MCP_API_KEY 未配置");
    }
    const mcpClient = await createMCPClient("ytdlp", ytdlp_url, ytdlp_api_key); // Add ID
    // clientInfo.push({ prefix: "ytdlp", client: mcpClient }); // Store info later
    mcpClientsToClose.push(mcpClient);

    const vidgen_url = process.env.VID_GEN_MCP_URL;
    const vidgen_api_key = process.env.VID_GEN_MCP_API_KEY;
    if (!vidgen_url || !vidgen_api_key) {
      throw new Error("VID_GEN_MCP_URL 或 VID_GEN_MCP_API_KEY 未配置");
    }
    const vidgenClient = await createMCPClient("vidgen", vidgen_url, vidgen_api_key); // Add ID
    // clientInfo.push({ prefix: "vidgen", client: vidgenClient }); // Store info later
    mcpClientsToClose.push(vidgenClient);

    const mcpToolsRaw = await mcpClient.tools();
    const vidgenToolsRaw = await vidgenClient.tools();

    // Store client info along with tools for easier lookup
    clientInfo.push({ prefix: "ytdlp", client: mcpClient});
    clientInfo.push({ prefix: "vidgen", client: vidgenClient});

    // Prefix tool names to avoid collisions and identify the correct client
    const prefixTools = (prefix: string, toolMap: Record<string, Tool<any, any>>): ToolSet => 
      Object.entries(toolMap).reduce((acc, [name, toolDef]) => {
        acc[`${prefix}__${name}`] = toolDef; // Assign the original tool definition
        return acc;
      }, {} as ToolSet); 


    tools = {
      ...prefixTools("ytdlp", mcpToolsRaw as Record<string, Tool<any, any>>), // Assert input type 
      ...prefixTools("vidgen", vidgenToolsRaw as Record<string, Tool<any, any>>),
    };
    logger.info(`合并后的工具列表: ${Object.keys(tools).join(', ')}`);


    // --- Fetch Project Context ---
    let projectContextString = "项目核心上下文不可用或获取失败。";
    let previousTasksString = "此项目先前没有已完成的任务。";
    if (readProjectFunction) {
      try {
        logger.info(`正在为任务 ${taskData.id} 获取项目 ${projectId} 的核心上下文...`);
        const projectContext: Project = await readProjectFunction(projectId);
        
        // --- 开始修改: 提取并格式化历史任务信息 ---
        const allTasks = projectContext.tasks || []; // 假设 Project 类型包含 tasks 数组
        
        // 筛选出当前任务之前已完成的任务，并按更新时间排序
        const previousCompletedTasks = allTasks
            .filter(task => task.id !== taskData.id && task.status === 'done')
            .sort((a, b) => (Number(a.updatedAt) || 0) - (Number(b.updatedAt) || 0)); // 按更新时间升序

        // 格式化历史任务信息字符串            
        if (previousCompletedTasks.length > 0) {
            previousTasksString = previousCompletedTasks.map(task => {
                return [
                    '  <previous_task>',
                    `    ID: ${task.id}`,
                    `    Title: ${task.title}`,
                    `    Description: ${task.description}`,
                    `    Status: ${task.status}`,
                    `    Result: ${task.completedDetails || '无结果详情。'}`, // task.completedDetails 可能为 null/undefined
                    '  </previous_task>'
                ].join('\n');
            }).join('\n\n');
        }
        // --- 结束修改 ---

        // 为了LLM提示，我们在这里只序列化项目本身，避免循环引用或过大的上下文
        const projectInfoForPrompt = { 
          projectId: projectContext.projectId,
          initialPrompt: projectContext.initialPrompt,
          projectPlan: projectContext.projectPlan,
          completed: projectContext.completed,
          autoApprove: projectContext.autoApprove,
          taskCount: projectContext.taskCount,
          // 不在此处包含 projectContext.tasks 或 projectContext.projectConclusion
          createdAt: projectContext.createdAt,
          updatedAt: projectContext.updatedAt
        };
        projectContextString = JSON.stringify(projectInfoForPrompt, null, 2);
        logger.info(`成功获取项目 ${projectId} 的核心上下文 (任务 ${taskData.id})`);
        await job.updateProgress(20);
      } catch (e) {
        logger.error(`获取项目 ${projectId} 核心上下文失败 (任务 ${taskData.id}):`, e);
        projectContextString = `获取项目核心上下文失败: ${e instanceof Error ? e.message : String(e)}`;
      }
    } else {
      logger.warn(`未提供 readProjectFunction 给 WorkerManager。无法获取任务 ${taskData.id} 的项目上下文。`);
    }

    const inProgressTaskData: BullMQTaskData = {
      ...taskData,
      status: "in progress",
      updatedAt: Date.now()
    };
    await job.updateData(inProgressTaskData);
    logger.info(`任务 ${taskData.id} 状态更新为 "in progress"`);
    
    // --- Prepare Initial Prompt and Messages ---
    const systemPrompt = `你好！在开始之前，请了解你可以使用以下工具来协助完成任务：

<available_tools>
- 视频下载工具 (ytdlp__*)：可下载数千个视频网站的视频、音频、字幕等数据。
- 视频发布工具 (vidgen__*)：可以把视频发布到托管网站上以便进行公开传播。
- FFmpeg执行工具 (可能在 vidgen__* 或其他 MCP 中)：一个可执行任意FFmpeg命令的工具，用于音视频和图片的编辑剪辑。
- 素材生成工具 (vidgen__*)：可生成图片和视频素材。
- STT 和 TTS (可能在 vidgen__* 或其他 MCP 中)：可执行语音转文字和文字转语音操作。
- WhisperX 执行工具 (可能在 vidgen__* 或其他 MCP 中)：可以对音频进行文本转录，或者生成标准的srt格式字幕，方便视频剪辑。
- 文本文件读写 (可能在 vidgen__* 或其他 MCP 中)：可以读写文本文件，方便进行文本分析和处理。
</available_tools>

当你调用工具操作具体文件的时候，请注意其输入文件名、输出文件名，并且在各个步骤之间做到文件名的衔接，也就是说，如果一个工具的输出文件名是另一个工具的输入文件名，请确保文件名是衔接的。绝对不要假设、虚构文件名。有时候你需要阅读前面所有的步骤以得到正确的文件名。

请注意：除了上述明确列出的工具，所有其他的思考、分析、决策和执行步骤都需要由你独立完成。

Tips：
* 了解一个视频的内容最快的方法是转录音频为文字并阅读它。
* 如果要调用ffmpeg操作字幕，推荐使用雅黑字体，路径位于 /mnt/c/Windows/Fonts/msyh.ttc
`;


    const userPrompt = `接下来，这里有一些关于当前项目的背景信息，以及一个需要你协助处理的具体任务。请先仔细阅读这些材料。

<project_context>
${projectContextString}
</project_context>

--- 先前任务历史 ---
${previousTasksString}
--- 历史任务结束 ---

然后，这是你需要处理的具体任务：
<current_task>
ID: ${taskData.id}
Title: ${taskData.title}
Description: ${taskData.description}
Status: ${taskData.status} // Note: Status is now \'in progress\'
Approved: ${taskData.approved}
${taskData.toolRecommendations ? `Tool Recommendations: ${taskData.toolRecommendations}` : ''}
${taskData.ruleRecommendations ? `Rule Recommendations: ${taskData.ruleRecommendations}` : ''}
</current_task>

现在，请你基于上述所有信息，逐步思考并执行完成这项任务。请告诉我你的第一个思考步骤或者需要调用的第一个工具。如果你完成了任务，请明确告诉我任务已完成，并提供最终的成果或结论。你的最终回复将被用作该任务的"完成详情"。`;

    const messages: CoreMessage[] = [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ];

    // --- Execution Loop ---
    const maxSteps = 12; // Maximum number of LLM calls/tool execution cycles
    let currentStep = 0;
    let finalResultText = "任务执行未产生最终文本结果。";

    while (currentStep < maxSteps) {
      currentStep++;
      logger.info(`任务 ${taskData.id} - 步骤 ${currentStep}/${maxSteps}: 调用 LLM...`);
      await job.updateProgress(30 + Math.round((currentStep / maxSteps) * 50)); // Progress update

      try {
        const result = await generateText({
          model: modelProvider,
          messages: [...messages], // Send a copy
          tools,
          // maxSteps in generateText is NOT what we want here for loop control
        });

        // Add the LLM\'s response (text or tool calls) to the history
        let assistantMessage: CoreMessage;
        if (result.finishReason === 'tool-calls') {
             // Ensure toolCalls is not undefined before assigning
            assistantMessage = { role: 'assistant', content: result.toolCalls ?? [] }; // Assign tool calls to content
        } else {
            // Includes 'stop', 'length', 'error', etc.
             // Ensure text is not undefined before assigning
             // Use nullish coalescing for safety
            assistantMessage = { role: 'assistant', content: result.text ?? '' };
        }
        messages.push(assistantMessage);

        if (result.finishReason === "stop") {
          logger.info(`任务 ${taskData.id} - 步骤 ${currentStep}: LLM 认为任务已完成。`);
          finalResultText = result.text;
          break; // Exit the loop
        } else if (result.finishReason === "tool-calls") {
          logger.info(`任务 ${taskData.id} - 步骤 ${currentStep}: LLM 请求工具调用: ${result.toolCalls.map(tc => tc.toolName).join(', ')}`);

          const toolResults: ToolResultPart[] = [];

          for (const toolCall of result.toolCalls) {
            const { toolCallId, toolName, args } = toolCall;
            logger.info(`任务 ${taskData.id} - 步骤 ${currentStep}: 执行工具 ${toolName} (ID: ${toolCallId})`);
            logger.debug(`工具参数: ${JSON.stringify(args)}`);

            const toolFn = tools[toolName] as Tool<any, any> | undefined; // Get the prefixed tool definition

            if (!toolFn) {
                logger.error(`任务 ${taskData.id} - 步骤 ${currentStep}: 找不到请求的工具 ${toolName}`);
                toolResults.push({
                    type: 'tool-result',
                    toolCallId,
                    toolName,
                    result: `错误: 未知的工具名称 "${toolName}"`,
                    isError: true,
                });
                continue; // Skip to next tool call if any
            }

            // Now that we\'ve checked toolFn exists, TypeScript should know it\'s defined
            // Also check if \'execute\' exists before calling
            if (typeof toolFn.execute !== 'function') {
                 logger.error(`任务 ${taskData.id} - 步骤 ${currentStep}: 工具 ${toolName} 没有可执行的 'execute' 方法。`);
                 toolResults.push({
                     type: 'tool-result',
                     toolCallId,
                     toolName,
                     result: `错误: 工具 ${toolName} 缺少 'execute' 方法。`,
                     isError: true,
                 });
                 continue;
            }

            try {
              // Explicitly await the potentially long-running tool call
              // Access the execute method on the Tool object
              // Pass the required ToolExecutionOptions
              const toolExecutionResult = await toolFn.execute(args, { toolCallId, messages });
              logger.info(`任务 ${taskData.id} - 步骤 ${currentStep}: 工具 ${toolName} (ID: ${toolCallId}) 执行成功。`);
              logger.debug(`工具结果: ${JSON.stringify(toolExecutionResult)}`); // Be careful logging potentially large results
              toolResults.push({
                type: "tool-result",
                toolCallId,
                toolName,
                result: toolExecutionResult, // Pass the actual result back
              });
            } catch (toolError) {
              logger.error( `任务 ${taskData.id} - 步骤 ${currentStep}: 工具 ${toolName} (ID: ${toolCallId}) 执行失败:`, toolError );
              toolResults.push({
                type: "tool-result",
                toolCallId,
                toolName,
                result: `工具执行错误: ${ toolError instanceof Error ? toolError.message : String(toolError) }`,
                isError: true,
              });
            }
          } // End of for loop iterating tool calls

          // Add all tool results to messages for the next LLM call
          messages.push({ role: 'tool', content: toolResults });

        } else {
          // Handle other finish reasons like \'length\', \'error\', etc.
          logger.warn(`任务 ${taskData.id} - 步骤 ${currentStep}: LLM 调用因 '${result.finishReason}' 结束。`);
          finalResultText = `LLM 调用意外结束: ${result.finishReason}. 部分结果: ${result.text || '无文本结果'}`;
          break; // Exit loop on unexpected finish
        }
      } catch (llmError) {
        logger.error(`任务 ${taskData.id} - 步骤 ${currentStep}: LLM 调用失败:`, llmError);
        finalResultText = `LLM 在步骤 ${currentStep} 调用失败: ${ llmError instanceof Error ? llmError.message : String(llmError) }`;
        // Potentially add the error message to history? Or just break?
         messages.push({ role: "assistant", content: `在尝试生成下一步时遇到错误: ${llmError instanceof Error ? llmError.message : String(llmError)}` });
        break; // Exit loop on LLM error
      }
    } // End of while loop

    if (currentStep >= maxSteps) {
      logger.warn(`任务 ${taskData.id} 已达到最大步骤数 ${maxSteps}。`);
      // Append a message indicating max steps reached if finalResultText wasn\'t set by a \'stop\'
       if (messages[messages.length - 1]?.role !== 'assistant' || !messages[messages.length - 1]?.content) {
           finalResultText = `任务执行达到最大步骤数 (${maxSteps}) 而未完成。最后的思考或工具结果请参见消息历史。`;
           messages.push({ role: "assistant", content: finalResultText });
       } else {
           finalResultText = messages[messages.length - 1].content as string // Use last assistant message
       }

    }

    logger.info(`任务 ${taskData.id} 执行循环完成。最终结果文本: ${finalResultText.substring(0,100)}...`);
    await job.updateProgress(90);

    // --- Finalize Task ---
    const completedData: BullMQTaskData = {
      ...taskData, // Start with original task data
      status: "done",
       // Store the final LLM text or a summary of the conversation
      completedDetails: finalResultText,
      // Optional: Store the full message history for debugging/auditing
      // executionHistory: messages,
      updatedAt: Date.now(),
    };

    await job.updateData(completedData);
    await job.updateProgress(100);
    logger.info(`任务 ${taskData.id} 处理完成，状态更新为 "done"。`);

    // --- Return Success Result ---
    return {
      taskId: taskData.id,
      status: "completed",
      completionTime: new Date().toISOString(),
      llmOutputSummary: finalResultText.substring(0, 200) + (finalResultText.length > 200 ? "..." : ""),
    };

  } catch (error) {
    logger.error(`处理任务 ${taskData.id} 失败 (在 processorFn 的最外层捕获):`, error);

    // --- Update Task to Failed ---
    const failedTaskData: BullMQTaskData = {
      ...taskData, // Start with original task data
      status: "failed" as BullMQTaskData['status'], // Changed from "not started" and added assertion
      completedDetails: `处理失败: ${ error instanceof Error ? error.message : String(error) }`,
      updatedAt: Date.now(),
    };

    try {
      await job.updateData(failedTaskData);
      logger.info(`任务 ${taskData.id} 状态更新为 "failed"。`);
    } catch (updateError) {
      logger.error( `更新任务 ${taskData.id} 数据为失败状态时再次出错:`, updateError );
    }

    // Rethrow the error to signal failure to BullMQ
    throw error;

  } finally {
    // --- Cleanup MCP Clients ---
    logger.info(
      `关闭 ${clientInfo.length} 个 MCP 客户端 (任务 ${taskData.id})...`
    );
    // Use the clientInfo array populated earlier
    for (const info of clientInfo) {
        const client = info.client;
        const prefix = info.prefix;
        try {
            if (typeof client.close === 'function') {
                await client.close();
                logger.info(`MCP 客户端 ${prefix} 关闭成功。`);
            } else {
                logger.warn(`客户端 ${prefix} 没有 close 方法。`);
            }
        } catch (closeError: unknown) {
            logger.error(
                `关闭 MCP 客户端 ${prefix} 时出错:`,
                closeError
            );
        }
    }
    mcpClientsToClose = []; // Clear the original array too
    clientInfo.length = 0; // Clear the info array
    logger.info(`MCP 客户端关闭完成 (任务 ${taskData.id})。`);
  }
}