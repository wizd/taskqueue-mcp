module.exports = {
  testEnvironment: 'node',
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1'
  },
  modulePathIgnorePatterns: ['<rootDir>/dist/'],
  // Force Jest to exit after all tests have completed
  forceExit: true,
  // Detect open handles and warn about them
  detectOpenHandles: true,
  // Extend the timeout to allow sufficient time for tests to complete
  testTimeout: 30000,
  // 添加setupFilesAfterEnv以确保在所有测试前加载dotenv
  setupFilesAfterEnv: ['<rootDir>/tests/jest-setup.mjs'],
  // 确保Jest处理ES模块
  transform: {
    '^.+\\.(ts|tsx)$': ['ts-jest', {
      useESM: true,
    }]
  },
  extensionsToTreatAsEsm: ['.ts']
}; 