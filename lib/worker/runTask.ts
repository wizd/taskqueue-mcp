import { generateText, experimental_createMCPClient } from "ai";
import { createMCPClient } from '../../lib/mcp/streamHttpClient.js';
import { BullMQTaskData } from "../../src/types/bullmq.js";
import { Logger } from '../../src/server/Logger.js';
import { Project } from "../../src/types/data.js";
import { modelProvider } from "../ai/provider.js";

export async function runTask(taskData: BullMQTaskData, projectId: string, job: any, logger: Logger, readProjectFunction?: (projectId: string) => Promise<Project>, finalizeProjectFunction?: (projectId: string, conclusion: string) => Promise<void>) {
    try {
        logger.info(`开始处理任务 ${taskData.id} (${taskData.title}) (项目: ${projectId})`);
        await job.updateProgress(10);
  
        // create mcp client
        let mcpClientsToClose: Awaited<
          ReturnType<typeof experimental_createMCPClient>
        >[] = [];
  
        const ytdlp_url = process.env.YTDLP_MCP_URL;
        const ytdlp_api_key = process.env.YTDLP_MCP_API_KEY;
        if (!ytdlp_url || !ytdlp_api_key) {
          throw new Error('YTDLP_MCP_URL 或 YTDLP_MCP_API_KEY 未配置');
        }
        const mcpClient = await createMCPClient("", ytdlp_url, ytdlp_api_key);
        mcpClientsToClose.push(mcpClient);
  
        const vidgen_url = process.env.VID_GEN_MCP_URL;
        const vidgen_api_key = process.env.VID_GEN_MCP_API_KEY;
        if (!vidgen_url || !vidgen_api_key) {
          throw new Error('VID_GEN_MCP_URL 或 VID_GEN_MCP_API_KEY 未配置');
        }
        const vidgenClient = await createMCPClient("", vidgen_url, vidgen_api_key);
        mcpClientsToClose.push(vidgenClient);
  
        const mcpTools = await mcpClient.tools();
        const vidgenTools = await vidgenClient.tools();

        console.log('mcpTools', mcpTools);
        console.log('vidgenTools', vidgenTools);

        const tools = {
          ...mcpTools,
          //...vidgenTools,
          //getWeather,
        };
        console.log('combined tools is ', tools);
  
        let projectContextString = "项目核心上下文不可用或获取失败。";
        if (readProjectFunction) {
          try {
            logger.info(`正在为任务 ${taskData.id} 获取项目 ${projectId} 的核心上下文...`);
            const projectContext: Project = await readProjectFunction(projectId);
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
        
        const llmPrompt = 
  `你好！在开始之前，请了解你可以使用以下工具来协助完成任务：
  
  <available_tools>
  - 视频下载工具：可下载数千个视频网站的视频、音频、字幕等数据。
  - FFmpeg执行工具：一个可执行任意FFmpeg命令的工具，用于音视频和图片的编辑剪辑。
  - 素材生成工具：基于Google Gemini，可生成图片和视频素材。
  </available_tools>

  当你调用工具操作具体文件的时候，请注意其输入文件名、输出文件名，并且在各个步骤之间做到文件名的衔接，也就是说，如果一个工具的输出文件名是另一个工具的输入文件名，请确保文件名是衔接的。绝对不要虚构文件名。
  
  请注意：除了上述明确列出的工具，所有其他的思考、分析、决策和执行步骤都需要由你独立完成。
  
  接下来，这里有一些关于当前项目的背景信息，以及一个需要你协助处理的具体任务。请先仔细阅读这些材料。
  
  <project_context>
  ${projectContextString}
  </project_context>
  
  然后，这是你需要处理的具体任务：
  <current_task>
  ID: ${taskData.id}
  Title: ${taskData.title}
  Description: ${taskData.description}
  Status: ${taskData.status}
  Approved: ${taskData.approved}
  ${taskData.toolRecommendations ? `Tool Recommendations: ${taskData.toolRecommendations}` : ''}
  ${taskData.ruleRecommendations ? `Rule Recommendations: ${taskData.ruleRecommendations}` : ''}
  </current_task>
  
  现在，请你基于上述所有信息，像在平时对话那样，告诉我你将如何完成这项任务。请详细描述你的思考过程、计划采取的步骤、关键的观察点（包括何时以及如何使用上述工具），以及预期的任务成果或结论。
  你的回复将被直接用作该任务的"完成详情"（completedDetails）记录下来。
  因此，请确保你的表述清晰、完整，并且紧扣任务的要求。谢谢！`;
        
        await job.updateProgress(30);
  
        let llmResultText = "LLM处理被跳过或遇到问题。使用默认完成详情。";
        try {
          logger.info(`开始为任务 ${taskData.id} 调用LLM...`);
        
          const { text: generatedText } = await generateText({
              model: modelProvider,
              prompt: llmPrompt,
              tools,
              maxSteps: 10,
              onStepFinish: async (step) => {
                console.log('onStepFinish');
              }
          });
          llmResultText = generatedText;
          logger.info(`LLM为任务 ${taskData.id} 推理成功。`);
          await job.updateProgress(80);
        } catch (llmError) {
          logger.error(`LLM为任务 ${taskData.id} 推理失败:`, llmError);
          llmResultText = `LLM推理失败: ${llmError instanceof Error ? llmError.message : String(llmError)}。原始任务描述: ${taskData.description}`;
        }
        
        const completedData: BullMQTaskData = {
          ...taskData,
          status: "done",
          completedDetails: llmResultText,
          updatedAt: Date.now()
        };
        
        await job.updateData(completedData);
        await job.updateProgress(100);
        
        logger.info(`任务 ${taskData.id} 处理完成，completedDetails已更新。`);
         
        console.log(
          `Closing ${mcpClientsToClose.length} MCP clients in onFinish...`,
        );
        for (const client of mcpClientsToClose) {
          try {
            await client.close();
          } catch (closeError: unknown) {
            console.error(
              'Error closing MCP client in onFinish:',
              closeError,
            );
          }
        }
        mcpClientsToClose = [];
  
        return {
          taskId: taskData.id,
          status: "completed",
          completionTime: new Date().toISOString(),
          llmOutputSummary: llmResultText.substring(0, 200) + (llmResultText.length > 200 ? "..." : "")
        };
      } catch (error) {
        logger.error(`处理任务 ${taskData.id} 失败 (在 processorFn 的最外层捕获):`, error);
        
        const failedTaskData: BullMQTaskData = {
          ...taskData,
          status: "not started",
          completedDetails: `处理失败: ${error instanceof Error ? error.message : String(error)}`,
          updatedAt: Date.now()
        };
        
        try {
          await job.updateData(failedTaskData);
        } catch (updateError) {
          logger.error(`更新任务 ${taskData.id} 数据为失败状态时再次出错:`, updateError);
        }
        
        throw error;
      }
}