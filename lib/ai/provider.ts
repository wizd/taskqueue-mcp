import { createXai } from '@ai-sdk/xai';
import { LanguageModel } from '@wizdy/ai';

const xai = createXai({
  apiKey: process.env.XAI_API_KEY,
});

const modelProvider: LanguageModel = xai("grok-3-mini");

export { modelProvider };
