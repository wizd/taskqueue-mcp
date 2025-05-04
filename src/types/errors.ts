// Error Codes
export enum AppErrorCode {
    // Protocol Errors (ERR_1xxx)
    MissingParameter = 'ERR_1000', // General missing param (mapped to protocol -32602)
    InvalidArgument = 'ERR_1002', // Extra param / invalid type (mapped to protocol -32602)

    // Validation / Resource Not Found (ERR_2xxx)
    ConfigurationError = 'ERR_2000', // e.g., Missing API Key for generate_project_plan
    ProjectNotFound = 'ERR_2001',
    TaskNotFound = 'ERR_2002',
    InvalidState = 'ERR_2003', // e.g., invalid state filter
    InvalidProvider = 'ERR_2004', // e.g., invalid model provider
    InvalidModel = 'ERR_2005', // e.g., invalid model name or model not accessible

    // No need for EmptyTaskFile code, handle during load
  
    // Business Logic / State Rules (ERR_3xxx)
    TaskNotDone = 'ERR_3000', // Cannot approve/finalize if task not done
    ProjectAlreadyCompleted = 'ERR_3001',
    // No need for CannotDeleteCompletedTask, handle in logic
    TasksNotAllDone = 'ERR_3003', // Cannot finalize project
    TasksNotAllApproved = 'ERR_3004', // Cannot finalize project
    CannotModifyApprovedTask = 'ERR_3005', // Added for clarity
    TaskAlreadyApproved = 'ERR_3006', // Added for clarity
  
    // File System (ERR_4xxx)
    FileReadError = 'ERR_4000', // Includes not found, permission denied etc.
    FileWriteError = 'ERR_4001',
    FileParseError = 'ERR_4002', // If needed during JSON parsing
    ReadOnlyFileSystem = 'ERR_4003',
  
    // LLM Interaction Errors (ERR_5xxx)
    LLMGenerationError = 'ERR_5000',
    LLMConfigurationError = 'ERR_5001', // Auth, key issues specifically with LLM provider call
  
    // BullMQ / Redis Errors (ERR_6xxx)
    RedisConnectionError = 'ERR_6000', // Redis连接错误
    QueueCreationError = 'ERR_6001', // 创建队列错误
    JobProcessingError = 'ERR_6002', // 作业处理错误
    RedisCommandError = 'ERR_6003', // Redis命令执行错误
    MigrationError = 'ERR_6004', // 数据迁移错误
    ServiceNotReadyError = 'ERR_6005', // BullMQ服务未就绪
    QueueNotFoundError = 'ERR_6006', // 找不到指定队列
    JobNotFoundError = 'ERR_6007', // 找不到特定作业
    StorageTypeError = 'ERR_6008', // 存储类型不匹配错误
  
    // Unknown / Catch-all (ERR_9xxx)
    Unknown = 'ERR_9999'
  }
  
  // Add a base AppError class
  export class AppError extends Error {
    public readonly code: AppErrorCode;
    public readonly details?: unknown;
  
    constructor(message: string, code: AppErrorCode, details?: unknown) {
      super(message);
      this.name = this.constructor.name; // Set name to the specific error class name
      this.code = code;
      this.details = details;
      
      // Fix prototype chain for instanceof to work correctly
      Object.setPrototypeOf(this, AppError.prototype);
    }
  }
  