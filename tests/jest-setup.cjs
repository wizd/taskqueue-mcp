// 在所有测试运行前加载.env环境变量
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs');

// __dirname变量在CommonJS模块中默认可用，不需要额外定义

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
    GEMINI_API_KEY: !!process.env.GEMINI_API_KEY,
    DEEPSEEK_API_KEY: !!process.env.DEEPSEEK_API_KEY
}); 