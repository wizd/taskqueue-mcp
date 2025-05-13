import { createXai } from '@ai-sdk/xai';
import { LanguageModel } from '@wizdy/ai';
import { createGoogleGenerativeAI } from '@ai-sdk/google';

const google = createGoogleGenerativeAI({

});

const xai = createXai({
  apiKey: process.env.XAI_API_KEY,
});

const modelProvider: LanguageModel = xai("grok-3-mini");
//google("gemini-2.5-flash-preview-04-17");
//xai("grok-3-mini");


export { modelProvider };
