/**
 * Logger类 - 简单的日志工具
 * 在实际项目中，可替换为更完善的日志库如winston或pino
 */
export class Logger {
  private prefix: string;
  
  /**
   * 创建Logger实例
   * @param prefix 日志前缀标识符
   */
  constructor(prefix: string) {
    this.prefix = prefix;
  }
  
  /**
   * 记录信息级别日志
   * @param message 日志消息
   * @param args 附加参数
   */
  info(message: string, ...args: any[]): void {
    console.log(`[${new Date().toISOString()}] [INFO] [${this.prefix}] ${message}`, ...args);
  }
  
  /**
   * 记录警告级别日志
   * @param message 日志消息
   * @param args 附加参数
   */
  warn(message: string, ...args: any[]): void {
    console.warn(`[${new Date().toISOString()}] [WARN] [${this.prefix}] ${message}`, ...args);
  }
  
  /**
   * 记录错误级别日志
   * @param message 日志消息
   * @param args 附加参数
   */
  error(message: string, ...args: any[]): void {
    console.error(`[${new Date().toISOString()}] [ERROR] [${this.prefix}] ${message}`, ...args);
  }
  
  /**
   * 记录调试级别日志
   * @param message 日志消息
   * @param args 附加参数
   */
  debug(message: string, ...args: any[]): void {
    // 仅当环境变量DEBUG设置为true或包含前缀时记录调试日志
    if (process.env.DEBUG === 'true' || 
        process.env.DEBUG === '*' || 
        (process.env.DEBUG && process.env.DEBUG.split(',').includes(this.prefix))) {
      console.debug(`[${new Date().toISOString()}] [DEBUG] [${this.prefix}] ${message}`, ...args);
    }
  }
} 