import { Task } from "./data.js";

// Define the structure for createProject success data
export interface ProjectCreationSuccessData {
    projectId: string;
    totalTasks: number;
    tasks: Array<{ id: string; title: string; description: string }>;
    message: string;
  }
  
  // --- Success Data Interfaces ---
  
  export interface ApproveTaskSuccessData {
    projectId: string;
    task: {
      id: string;
      title: string;
      description: string;
      completedDetails: string;
      approved: boolean;
    };
  }
  
  export interface ApproveProjectSuccessData {
    projectId: string;
    message: string;
  }
  
  export interface OpenTaskSuccessData {
    projectId: string;
    task: Task;
  }
  
  export interface ListProjectsSuccessData {
    message: string;
    projects: Array<{
      projectId: string;
      initialPrompt: string;
      totalTasks: number;
      completedTasks: number;
      approvedTasks: number;
      tenantId?: string;
      createdAt?: string;
      updatedAt?: string;
    }>;
  }
  
  export interface ListTasksSuccessData {
    message: string;
    tasks: Task[]; // Use the full Task type
  }
  
  export interface AddTasksSuccessData {
    message: string;
    newTasks: Array<{ id: string; title: string; description: string }>;
  }
  
  export interface DeleteTaskSuccessData {
    message: string;
  }

  export interface ReadProjectSuccessData {
    projectId: string;
    initialPrompt: string;
    projectPlan: string;
    completed: boolean;
    autoApprove?: boolean;
    tasks: Task[];
    taskCount?: number;
    projectConclusion?: string;
    tenantId?: string;
    createdAt?: string;
    updatedAt?: string;
  }

  // Add the new interface for update_task success
  export interface UpdateTaskSuccessData {
    task: Task; // The updated task object
    message?: string; // Optional message (e.g., approval reminder)
  }
  