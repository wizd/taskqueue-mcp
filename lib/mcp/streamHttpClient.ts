import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { experimental_createMCPClient } from 'ai';

export async function createStreamHttpClient(tenantId: string, apiUrl: string, apiKey: string) : Promise<any> {
    // 获取 bearer token
    const bearerToken = apiKey;
    
    if (!bearerToken) {
      console.error('API Key 未提供');
      throw new Error('API密钥未配置');
    }
    if (!apiUrl) {
      console.error('API URL 未提供');
      throw new Error('API URL 未配置');
    }
    
    // 设置认证选项
    const transportOptions = {
      requestInit: {
        headers: {
          'Authorization': `Bearer ${bearerToken}`
        }
      }
    };

    // 使用认证选项创建 transport
    const transport = new StreamableHTTPClientTransport(
      tenantId ? new URL(`${apiUrl}/${tenantId}`) : new URL(apiUrl),
      transportOptions
    );
    
    const customClient = await experimental_createMCPClient({
      transport,
    });

    return customClient;
}