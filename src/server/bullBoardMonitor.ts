import express from 'express';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
// @ts-ignore
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js'; // 使用原始路径，并忽略 TS 错误
import { Queue, QueueEvents } from 'bullmq';
import { RedisManager } from './RedisManager.js';
import { createRedisKeys, normalizeRedisPrefix } from '../types/bullmq.js';
import { AppError, AppErrorCode } from '../types/errors.js';

// Store adapters mapped by normalized queue name for easier removal
const queueAdapters = new Map<string, BullMQAdapter>();
// Keep pendingQueues for initialization logic
const pendingQueues: Array<[string, string, string | undefined, any]> = []; // Add prefix to pending queue data
// Store server adapter reference
let serverAdapter: ExpressAdapter | null = null;
// Store whether initialized
let isInitialized = false;
// Store bull-board API methods reference
let bullBoardApi: { 
  addQueue: (adapter: BullMQAdapter) => void;
  removeQueue: (adapter: BullMQAdapter | string) => void; // Allow removing by name or adapter
} | null = null;
// Redis keyspace notification listener
let keyspaceNotificationListener: QueueEvents | null = null;

/**
 * Creates and adds a queue adapter to the board based on projectId and prefix.
 * @param projectId The project ID.
 * @param prefix The Redis prefix (e.g., 'tenant:xyz:').
 * @param redis Redis connection.
 * @param skipIfNotInitialized If true, add to pending list if board isn't ready.
 * @returns The created adapter or null.
 */
async function createAndAddQueueAdapter(
  projectId: string,
  prefix: string | undefined,
  redis: any,
  skipIfNotInitialized: boolean = false
): Promise<BullMQAdapter | null> {
  // Generate the exact queue name using the same logic as BullMQService
  const redisKeys = createRedisKeys(normalizeRedisPrefix(prefix));
  const normalizedQueueName = redisKeys.projectQueueName(projectId);
  
  try {
    // Check if adapter already exists using the normalized name
    if (queueAdapters.has(normalizedQueueName)) {
      return queueAdapters.get(normalizedQueueName) || null;
    }

    // Prepare queue options - IMPORTANT: use empty prefix for the Queue constructor
    let queueOptions: any = {
      connection: redis,
      prefix: '' // BullMQ Queue instance uses the full name, no internal prefix needed
    };

    // Handle case where Bull Board is not yet initialized
    if ((!serverAdapter || !isInitialized || !bullBoardApi) && skipIfNotInitialized) {
      pendingQueues.push([normalizedQueueName, projectId, prefix, queueOptions]);
      console.log(`Bull Board: Queue ${normalizedQueueName} added to pending list (Project: ${projectId}, Prefix: ${prefix || 'None'})`);
      return null;
    }

    if (!serverAdapter || !isInitialized || !bullBoardApi) {
      console.warn(`Bull Board: Attempted to add adapter for queue ${normalizedQueueName}, but board is not initialized.`);
      return null;
    }

    console.log(`Bull Board: Creating adapter for queue ${normalizedQueueName} (Project: ${projectId}, Prefix: ${prefix || 'None'})`);

    // Create the queue instance with the full, normalized name
    const queue = new Queue(normalizedQueueName, queueOptions);
    const adapter = new BullMQAdapter(queue);

    // Store the adapter mapped by its full normalized name
    queueAdapters.set(normalizedQueueName, adapter);

    // Add adapter to the board
    bullBoardApi.addQueue(adapter);
    console.log(`Bull Board: Successfully added queue ${normalizedQueueName} to the monitoring panel.`);

    return adapter;
  } catch (error) {
    console.error(`Bull Board: Error adding adapter for queue ${normalizedQueueName}:`, error);
    return null;
  }
}

/**
 * Process pending queues after Bull Board initialization.
 */
async function processPendingQueues(redis: any): Promise<number> {
  let processed = 0;
  if (pendingQueues.length > 0) {
    console.log(`Bull Board: Processing ${pendingQueues.length} pending queues...`);
    
    while (pendingQueues.length > 0) {
      // Retrieve all data including prefix
      const [normalizedQueueName, projectId, prefix, queueOptions] = pendingQueues.shift()!; 
      
      try {
        if (!queueAdapters.has(normalizedQueueName)) {
          // Ensure queue options use empty prefix
          queueOptions.prefix = ''; 
          
          const queue = new Queue(normalizedQueueName, queueOptions);
          const adapter = new BullMQAdapter(queue);
          
          queueAdapters.set(normalizedQueueName, adapter); // Map by name
          
          bullBoardApi!.addQueue(adapter);
          processed++;
          
          console.log(`Bull Board: Successfully added pending queue ${normalizedQueueName} to panel (Project: ${projectId}, Prefix: ${prefix || 'None'})`);
        }
      } catch (error) {
        console.error(`Bull Board: Error processing pending queue ${normalizedQueueName}:`, error);
      }
    }
  }
  return processed;
}

/**
 * Scan Redis for project queues based on metadata keys.
 * Note: This might need adjustment based on how projects/queues are created across prefixes.
 * It currently uses the prefix defined in RedisKeys constants, which might not cover all tenants.
 * Consider passing the current prefix or iterating through known tenant prefixes if needed.
 */
async function discoverAllQueues(redis: any, skipIfNotInitialized: boolean = false): Promise<number> {
  try {
    let addedCount = 0;
    let discoveredCount = 0;

    // *** This discovery logic might need refinement for multi-tenancy ***
    // It currently uses the default RedisKeys which might not have a prefix.
    // To discover tenant queues, you might need to scan with tenant-specific patterns.
    console.warn("Bull Board: discoverAllQueues might not find queues created with specific tenant prefixes.");

    // Example using default prefix (likely won't find tenant queues)
    const defaultRedisKeys = createRedisKeys(); // No prefix
    const standardKeys = await redis.keys(defaultRedisKeys.projectMetadataPattern());
    
    if (standardKeys && standardKeys.length > 0) {
      console.log(`Bull Board: Discovered ${standardKeys.length} project metadata keys (using default pattern).`);
      discoveredCount += standardKeys.length;

      for (const key of standardKeys) {
        const match = key.match(defaultRedisKeys.projectIdFromKeyRegex());
        const projectId = match ? match[1] : null;

        if (projectId) {
          // Add using the default prefix (or lack thereof)
          const adapter = await createAndAddQueueAdapter(projectId, undefined, redis, skipIfNotInitialized);
          if (adapter) addedCount++;
        }
      }
    }
    
    // TODO: Add logic here to scan for tenant-specific queues if needed.
    // Example: Iterate known tenant prefixes or use a broader SCAN pattern.

    console.log(`Bull Board: Queue discovery finished. Discovered: ${discoveredCount}, Added/Updated: ${addedCount}`);
    return addedCount;
  } catch (error) {
    console.error('Bull Board: Error discovering queues:', error);
    return 0;
  }
}

/**
 * Setup listeners for queue events (e.g., creation/deletion via keyspace notifications).
 * Requires Redis keyspace notifications to be enabled.
 */
async function setupQueueDiscovery(redis: any) {
    // ... (Implementation might need adjustment for multi-tenant discovery) ...
    console.warn("Bull Board: setupQueueDiscovery (keyspace notifications) might not detect queues created with specific tenant prefixes unless Redis listener pattern is adjusted.");
}

/**
 * Starts the Bull Board UI.
 * @param app Express application instance.
 * @param basePath Base path for the UI.
 */
export function setupBullBoard(app: express.Express, basePath = '/admin/queues') {
  if (serverAdapter) {
    console.warn('Bull Board already initialized.');
    return serverAdapter;
  }

  console.log(`Setting up Bull Board UI at ${basePath}`);
  serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath(basePath);

  const redisManager = RedisManager.getInstance();
  const redisConnection = redisManager.getConnection();

  const { addQueue, removeQueue, setQueues } = createBullBoard({
    queues: [], // Initialize with empty array, queues added dynamically
    serverAdapter: serverAdapter,
    options: {
      uiConfig: {
        boardTitle: 'TaskQueue MCP Queues',
        // ... other UI config
      }
    }
  });

  bullBoardApi = { addQueue, removeQueue };
  isInitialized = true;
  console.log('Bull Board API initialized.');

  // Process any queues that were pending before initialization
  processPendingQueues(redisConnection).then(count => {
    if (count > 0) console.log(`Bull Board: Processed ${count} pending queues.`);
  });

  // Setup initial discovery and potentially keyspace listeners
  discoverAllQueues(redisConnection, false); // Discover existing queues on startup
  // setupQueueDiscovery(redisConnection); // Optional: Listen for new queues

  app.use(basePath, serverAdapter.getRouter());
  console.log(`Bull Board UI is available at ${basePath}`);
  
  return serverAdapter;
}

/**
 * Adds a specific project's queue to the Bull Board.
 * Called by BullMQService when a queue is potentially created.
 * @param projectId The project ID.
 * @param prefix The Redis prefix used when creating the project/queue.
 */
export async function addQueueToBoard(projectId: string, prefix?: string): Promise<void> { 
  if (!isInitialized || !serverAdapter) {
    console.warn('Bull Board not initialized, queue will be added to pending list.');
    // Get connection for potential pending queue addition
    const redisManager = RedisManager.getInstance();
    const redisConnection = redisManager.getConnection();
    await createAndAddQueueAdapter(projectId, prefix, redisConnection, true); // Add to pending list
    return;
  }

  try {
    const redisManager = RedisManager.getInstance();
    const redisConnection = redisManager.getConnection();
    await createAndAddQueueAdapter(projectId, prefix, redisConnection, false);
  } catch (error) {
    console.error(`Bull Board: Failed to explicitly add queue for project ${projectId} (Prefix: ${prefix || 'None'}):`, error);
    // Do not throw, let the service continue
  }
}

/**
 * Removes a specific project's queue from the Bull Board.
 * Called by BullMQService when a project is deleted.
 * @param projectId The project ID.
 * @param prefix The Redis prefix used when the project/queue was created.
 */
export async function removeQueueFromBoard(projectId: string, prefix?: string): Promise<void> { 
  if (!isInitialized || !bullBoardApi || !serverAdapter) {
    console.warn('Bull Board not initialized, cannot remove queue.');
    // Also remove from pending list if it's there
    const redisKeys = createRedisKeys(normalizeRedisPrefix(prefix));
    const normalizedQueueName = redisKeys.projectQueueName(projectId);
    const pendingIndex = pendingQueues.findIndex(q => q[0] === normalizedQueueName);
    if (pendingIndex > -1) {
      pendingQueues.splice(pendingIndex, 1);
      console.log(`Bull Board: Removed queue ${normalizedQueueName} from pending list.`);
    }
    return;
  }

  try {
    // Generate the exact queue name
    const redisKeys = createRedisKeys(normalizeRedisPrefix(prefix));
    const normalizedQueueName = redisKeys.projectQueueName(projectId);

    const adapterToRemove = queueAdapters.get(normalizedQueueName);

    if (adapterToRemove) {
      console.log(`Bull Board: Removing queue ${normalizedQueueName} from panel...`);
      bullBoardApi.removeQueue(adapterToRemove); // Use the adapter instance
      
      // Attempt to close the underlying queue connection
      try {
          // @ts-ignore - Accessing queue property which should exist on BullMQAdapter
          if (adapterToRemove.queue && typeof adapterToRemove.queue.close === 'function') { 
             // @ts-ignore
             await adapterToRemove.queue.close(); 
          }
      } catch (closeError) {
          console.warn(`Bull Board: Error closing queue connection for ${normalizedQueueName} during removal:`, closeError);
      }

      queueAdapters.delete(normalizedQueueName);
      console.log(`Bull Board: Successfully removed queue ${normalizedQueueName}.`);
    } else {
      console.warn(`Bull Board: Attempted to remove queue ${normalizedQueueName}, but it was not found in the adapter map.`);
    }
  } catch (error) {
    console.error(`Bull Board: Error removing queue for project ${projectId} (Prefix: ${prefix || 'None'}):`, error);
    // Do not throw, let the service continue
  }
} 