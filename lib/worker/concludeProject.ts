import { generateText } from "ai";
import { Logger } from "../../src/server/Logger.js";
import { Project } from "../../src/types/data.js";
import { modelProvider } from "../ai/provider.js";

export async function checkAndConcludeProject(projectId: string, logger: Logger, readProjectFunction?: (projectId: string) => Promise<Project>, finalizeProjectFunction?: (projectId: string, conclusion: string) => Promise<void>): Promise<void> {
    if (!readProjectFunction) {
      logger.warn(`[ProjectConclude] 未提供 readProjectFunction，无法检查项目 ${projectId} 是否完成。`);
      return;
    }
    if (!finalizeProjectFunction) {
        logger.warn(`[ProjectConclude] 未提供 finalizeProjectFunction，无法完成项目 ${projectId}。`);
        return;
    }

    try {
      logger.info(`[ProjectConclude] 检查项目 ${projectId} 是否所有任务都已完成...`);
      const project = await readProjectFunction(projectId);

      if (project.completed) {
        logger.info(`[ProjectConclude] 项目 ${projectId} 已经标记为完成，跳过总结。`);
        return;
      }

      const allTasksDone = project.tasks && project.tasks.every(task => task.status === "done");

      if (allTasksDone && project.tasks.length > 0) { // 确保有任务且所有任务都完成
        logger.info(`[ProjectConclude] 项目 ${projectId} 所有任务均已完成，开始执行项目总结。`);
        await concludeProject(projectId, project, logger, finalizeProjectFunction); // 传递 project 对象以避免再次读取
      } else {
        if (project.tasks.length === 0) {
             logger.info(`[ProjectConclude] 项目 ${projectId} 没有任务，考虑直接标记完成或通过其他流程处理。当前跳过自动总结。`);
        } else {
            logger.info(`[ProjectConclude] 项目 ${projectId} 尚有未完成的任务，不执行项目总结。`);
        }
      }
    } catch (error) {
      logger.error(`[ProjectConclude] 检查或执行项目 ${projectId} 总结时出错:`, error);
    }
  }

export async function concludeProject(projectId: string, projectData: Project, logger: Logger, finalizeProjectFunction?: (projectId: string, conclusion: string) => Promise<void>): Promise<void> {
    logger.info(`[ConcludeProject] 开始为项目 ${projectId} 生成总结...`);

    // 准备项目所有任务的详情字符串
    let tasksDetailsString = projectData.tasks.map(task => 
      `任务ID: ${task.id}\n标题: ${task.title}\n状态: ${task.status}\n审批状态: ${task.approved}\n完成详情: ${task.completedDetails || '无'}\n---`
    ).join('\n\n');
    if (!tasksDetailsString) tasksDetailsString = "该项目没有任务或未能获取任务详情。";

    const projectContextForLLM = JSON.stringify({ 
        projectId: projectData.projectId,
        initialPrompt: projectData.initialPrompt,
        projectPlan: projectData.projectPlan,
        autoApprove: projectData.autoApprove,
        taskCount: projectData.tasks.length,
        createdAt: projectData.createdAt,
        updatedAt: projectData.updatedAt,
     }, null, 2);

    const llmPrompt = 
`你好！以下是一个项目的完整上下文信息，包括其所有任务的处理结果。

<project_overview>
${projectContextForLLM}
</project_overview>

<all_tasks_details>
${tasksDetailsString}
</all_tasks_details>

现在，所有任务均已处理完毕。请你基于以上所有信息，为整个项目撰写一份最终的总结报告。
这份报告应该概述项目的主要成果、遇到的挑战（如果有）、关键的学习点以及项目的整体完成情况。
你的回复将作为项目的最终总结（projectConclusion）被保存下来。
请确保内容全面、精炼，并能准确反映项目的整个生命周期。谢谢！`;

    let projectLlmConclusion = "LLM项目总结失败或被跳过。";
    try {
      logger.info(`[ConcludeProject] 调用LLM为项目 ${projectId} 生成总结...`);

      const { text: generatedConclusion } = await generateText({
        model: modelProvider,
        prompt: llmPrompt,
      });
      projectLlmConclusion = generatedConclusion;
      logger.info(`[ConcludeProject] LLM为项目 ${projectId} 生成总结成功。`);
    } catch (llmError) {
      logger.error(`[ConcludeProject] LLM为项目 ${projectId} 生成总结失败:`, llmError);
      projectLlmConclusion = `LLM项目总结失败: ${llmError instanceof Error ? llmError.message : String(llmError)}`;
    }

    if (finalizeProjectFunction) {
      try {
        logger.info(`[ConcludeProject] 调用 finalizeProjectFunction 保存项目 ${projectId} 的总结并标记完成...`);
        await finalizeProjectFunction(projectId, projectLlmConclusion);
        logger.info(`[ConcludeProject] 项目 ${projectId} 总结已保存并标记为完成。`);
      } catch (finalizeError) {
        logger.error(`[ConcludeProject] 调用 finalizeProjectFunction 完成项目 ${projectId} 时出错:`, finalizeError);
      }
    } else {
      logger.error(`[ConcludeProject] finalizeProjectFunction 未定义，无法完成项目 ${projectId}。`);
    }
  }