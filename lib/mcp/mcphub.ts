import { createStreamHttpClient } from './streamHttpClient';
import type { experimental_createMCPClient as createMCPClientType } from 'ai';

// Define the type for the client returned by createStreamHttpClient
// This is essentially Awaited<ReturnType<typeof experimental_createMCPClient>>
// We'll refer to the specific Tool type from the Vercel AI SDK if possible, or define a compatible one.
type IndividualMCPClient = Awaited<ReturnType<typeof createStreamHttpClient>>;

// Define the structure of a tool, compatible with what client.tools() returns
// Based on Vercel AI SDK, a tool has description, parameters (zod schema), and execute
interface ToolDefinition<Parameters = any, Result = any> {
  description?: string;
  parameters?: Parameters; // Typically a Zod schema
  execute: (args: Parameters extends undefined ? void : Parameters) => Promise<Result>;
  // The actual Tool type from 'ai' package is Tool<Name, ParametersSchema, Result>
  // but when we get it from .tools(), the name is the key.
}

export interface MCPHubClientConfigItem {
  /** Unique identifier for this client configuration (e.g., tenantId or a custom alias) */
  id: string;
  apiUrl: string;
  apiKey: string;
  /** Optional tenantId, passed to createStreamHttpClient for URL construction */
  tenantId?: string;
}

export class MCPHubClient {
  private clientConfigs: MCPHubClientConfigItem[];
  private initializedClients: Map<string, IndividualMCPClient> = new Map();
  private aggregatedTools: Record<string, ToolDefinition> = {};
  private isInitialized = false;

  constructor(configs: MCPHubClientConfigItem[]) {
    this.clientConfigs = configs;
  }

  /**
   * Initializes all configured MCP clients and aggregates their tools.
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      console.log('MCPHubClient is already initialized.');
      return;
    }

    console.log(`MCPHubClient initializing with ${this.clientConfigs.length} configurations...`);

    for (const config of this.clientConfigs) {
      try {
        // Pass config.tenantId (which can be undefined) or an empty string if createStreamHttpClient expects a string.
        // The user's streamHttpClient handles tenantId being optional for the URL.
        const client = await createStreamHttpClient(config.tenantId || '', config.apiUrl, config.apiKey);
        this.initializedClients.set(config.id, client);
        console.log(`Successfully initialized client for config ID: ${config.id}`);

        // Type assertion for clientTools if needed, or ensure createStreamHttpClient's return type is accurate.
        const clientTools: Record<string, ToolDefinition> = await client.tools() as Record<string, ToolDefinition>;
        
        for (const toolName in clientTools) {
          if (Object.prototype.hasOwnProperty.call(clientTools, toolName)) {
            if (this.aggregatedTools[toolName]) {
              console.warn(`Tool name conflict: Tool '${toolName}' from client '${config.id}' is overwriting a previous definition (from client associated with the first instance of this tool).`);
            }
            // The tool object from clientTools (clientTools[toolName]) should have its 'execute' method
            // correctly bound to its parent client instance by the Vercel AI SDK.
            this.aggregatedTools[toolName] = clientTools[toolName];
          }
        }
      } catch (error) {
        console.error(`Failed to initialize MCP client for config ID '${config.id}':`, error);
        // Depending on requirements, you might want to throw here or collect errors.
      }
    }
    this.isInitialized = true;
    console.log(`MCPHubClient finished initialization. ${this.initializedClients.size} clients successfully initialized. Found ${Object.keys(this.aggregatedTools).length} unique tools.`);
  }

  /**
   * Returns an aggregated collection of all tools from all initialized clients.
   * The 'execute' method of each tool is automatically routed to the correct underlying client.
   * @throws Error if the hub is not initialized.
   */
  public async tools(): Promise<Record<string, ToolDefinition>> {
    if (!this.isInitialized) {
      // Alternative: await this.initialize(); if auto-initialization on first use is desired.
      throw new Error("MCPHubClient is not initialized. Call initialize() first.");
    }
    return this.aggregatedTools;
  }

  /**
   * Retrieves a specific initialized client instance by its configuration ID.
   * @param clientId The ID of the client configuration.
   * @returns The client instance, or undefined if not found or not initialized.
   * @throws Error if the hub is not initialized.
   */
  public getClientById(clientId: string): IndividualMCPClient | undefined {
    if (!this.isInitialized) {
      throw new Error("MCPHubClient is not initialized. Call initialize() first.");
    }
    return this.initializedClients.get(clientId);
  }

  /**
   * Checks if the hub has completed its initialization process.
   */
  public getIsInitialized(): boolean {
    return this.isInitialized;
  }
}

/**
 * Factory function to create an MCPHubClient instance.
 * @param configs Array of configurations for the underlying MCP clients.
 */
export function createMCPHubClient(configs: MCPHubClientConfigItem[]): MCPHubClient {
  return new MCPHubClient(configs);
}

// Default export can be useful for certain module systems or preferences.
export default {
  createMCPHubClient,
  MCPHubClient
};