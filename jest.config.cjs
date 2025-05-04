module.exports = {
  preset: 'ts-jest/presets/js-with-ts-esm',
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
  setupFilesAfterEnv: ['<rootDir>/tests/jest-setup.cjs'],
  // 确保Jest处理ES模块
  transform: {
    '^.+\\.m?[jt]sx?$': ['ts-jest', {
      useESM: true,
    }]
  },
  transformIgnorePatterns: [
    'node_modules/(?!(jest-)?@modelcontextprotocol|@ai-sdk)'
  ],
  extensionsToTreatAsEsm: ['.ts'],
  testMatch: [
    '**/__tests__/**/*.ts?(x)',
    '**/?(*.)+(spec|test).ts?(x)'
  ]
};
