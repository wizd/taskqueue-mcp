import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { experimental_createMCPClient } from 'ai';
import { MCPClient } from 'ai';

export async function createMCPClient(tenantId: string, apiUrl: string, apiKey: string) : Promise<MCPClient> {
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

    let transport : Transport;

    console.log("api url", apiUrl);
    const url = tenantId ? new URL(`${apiUrl}/${tenantId}`) : new URL(apiUrl);
    if(url.protocol === 'ws:'){
      const client = await experimental_createMCPClient({
        transport: {
          type: 'ws',
          url: url.toString(),
          headers: {
            'Authorization': `Bearer ${bearerToken}`
          }
        }
      });
      return client;
    }else{
      // 设置认证选项
      const transportOptions = {
        requestInit: {
          headers: {
            'Authorization': `Bearer ${bearerToken}`
          }
        }
      };

      transport = new StreamableHTTPClientTransport(
        tenantId ? new URL(`${apiUrl}/${tenantId}`) : new URL(apiUrl),
        transportOptions
      );

      const customClient = await experimental_createMCPClient({
        transport,
      });
  
      return customClient;
    }   
}