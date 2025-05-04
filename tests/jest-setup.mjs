// 在所有测试运行前加载.env环境变量
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { createRequire } from 'module';

// 使用ES模块的方法获取当前文件路径
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const require = createRequire(import.meta.url);

// 尝试加载项目根目录下的.env文件
const envPath = path.resolve(path.join(__dirname, '..'), '.env');

// 检查.env文件是否存在
if (fs.existsSync(envPath)) {
    console.log(`加载环境变量文件: ${envPath}`);
    const result = dotenv.config({ path: envPath });

    if (result.error) {
        console.error('加载.env文件失败:', result.error);
    } else {
        console.log('成功加载环境变量');
    }
} else {
    console.warn(`警告: .env文件不存在于路径 ${envPath}`);
}

// 记录重要的API密钥是否已加载（不显示实际值）
console.log('测试环境变量状态:', {
    OPENAI_API_KEY: !!process.env.OPENAI_API_KEY,
    GOOGLE_GENERATIVE_AI_API_KEY: !!process.env.GOOGLE_GENERATIVE_AI_API_KEY,
    DEEPSEEK_API_KEY: !!process.env.DEEPSEEK_API_KEY
}); 