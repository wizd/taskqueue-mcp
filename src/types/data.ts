// Task and Project Interfaces
export interface Task {
    id: string;
    title: string;
    description: string;
    status: "not started" | "in progress" | "done";
    approved: boolean;
    completedDetails: string;
    toolRecommendations?: string;
    ruleRecommendations?: string;
    tenantId?: string;
    createdAt?: string;
    updatedAt?: string;
  }
  
  export interface Project {
    projectId: string;
    initialPrompt: string;
    projectPlan: string;
    tasks: Task[];
    completed: boolean;
    autoApprove?: boolean;
    taskCount?: number;
    projectConclusion?: string;
    tenantId?: string;
    createdAt?: string;
    updatedAt?: string;
  }
  
  export interface TaskManagerFile {
    projects: Project[];
  }
  
  // Define valid task status transitions
  export const VALID_STATUS_TRANSITIONS = {
    "not started": ["in progress"],
    "in progress": ["done", "not started"],
    "done": ["in progress"]
  } as const;
  
  export type TaskState = "open" | "pending_approval" | "completed" | "all";
  