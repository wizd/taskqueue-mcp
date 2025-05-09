import { createXai } from '@ai-sdk/xai';

const xai = createXai({
  apiKey: process.env.XAI_API_KEY,
});

const modelProvider = xai("grok-3-mini");

export { modelProvider };
