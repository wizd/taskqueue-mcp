/**
 * Redis命名验证工具
 * 用于处理Redis键名的规范化和验证
 */

import { normalizeRedisPrefix } from "../types/bullmq.js";

/**
 * Redis键名验证器
 * 提供静态方法处理和验证各种Redis键
 */
export class RedisNamingValidator {
  /**
   * 规范化租户ID格式
   * 移除前导冒号和空格
   * @param tenantId 原始租户ID
   * @returns 规范化后的租户ID
   */
  public static normalizeTenantId(tenantId?: string): string | undefined {
    if (!tenantId) return undefined;
    
    // 移除前导冒号和空格
    let normalized = tenantId.trim().replace(/^:+/, '');
    
    // 如果租户ID变化了，记录警告
    if (normalized !== tenantId) {
      console.warn(`规范化租户ID: ${tenantId} -> ${normalized}`);
    }
    
    return normalized;
  }
  
  /**
   * 创建规范化的队列名
   * 确保使用正确的租户_项目格式
   * @param tenantId 租户ID
   * @param projectId 项目ID
   * @returns 规范化的队列名
   */
  public static createQueueName(tenantId: string | undefined, projectId: string): string {
    const normalizedTenantId = this.normalizeTenantId(tenantId);
    
    if (!normalizedTenantId) {
      return `proj_${projectId}`;
    }
    
    return `tenant_${normalizedTenantId}_proj_${projectId}`;
  }
  
  /**
   * 验证队列名是否符合规范格式
   * @param queueName 队列名
   * @param tenantId 期望的租户ID
   * @param projectId 期望的项目ID
   * @returns 如果队列名符合规范返回true，否则返回false
   */
  public static validateQueueName(queueName: string, tenantId: string | undefined, projectId: string): boolean {
    // 移除前导冒号
    const normalizedQueueName = queueName.replace(/^:+/, '');
    
    // 创建期望的队列名
    const expectedQueueName = this.createQueueName(tenantId, projectId);
    
    return normalizedQueueName === expectedQueueName;
  }
  
  /**
   * 修复队列名
   * @param queueName 原始队列名
   * @param tenantId 租户ID (可选)
   * @returns 修复后的队列名
   */
  public static fixQueueName(queueName: string, tenantId?: string): string {
    // 1. 移除前导冒号
    let fixedName = queueName.replace(/^:+/, '');
    
    // 2. 从队列名提取租户ID和项目ID
    const tenantMatch = fixedName.match(/tenant[_:]([^_:]+)[_:]/);
    const projectMatch = fixedName.match(/proj[_:]+(proj-\d+)/);
    
    // 如果能够提取租户ID和项目ID，创建标准队列名
    if (projectMatch && projectMatch[1]) {
      const extractedProjectId = projectMatch[1];
      
      // 优先使用队列名中的租户ID，如果没有则使用传入的租户ID
      const extractedTenantId = tenantMatch ? tenantMatch[1] : tenantId;
      
      // 创建标准格式的队列名
      const standardName = this.createQueueName(extractedTenantId, extractedProjectId);
      
      // 如果名称有变化，记录警告
      if (standardName !== fixedName) {
        console.warn(`修复队列名: ${queueName} -> ${standardName}`);
      }
      
      return standardName;
    }
    
    // 如果无法提取项目ID，至少移除前导冒号
    if (fixedName !== queueName) {
      console.warn(`移除队列名前导冒号: ${queueName} -> ${fixedName}`);
    }
    
    return fixedName;
  }
  
  /**
   * 创建Redis键名
   * 用于创建标准格式的Redis键
   * @param prefix 前缀
   * @param parts 键名组成部分
   * @returns 标准格式的Redis键
   */
  public static createRedisKey(prefix: string | undefined, ...parts: string[]): string {
    const normalizedPrefix = normalizeRedisPrefix(prefix);
    return `${normalizedPrefix || ''}${parts.join(':')}`;
  }
}
