// eslint.config.js
import { ESLint } from 'eslint';

export default new ESLint({
  baseConfig: {
    parser: '@typescript-eslint/parser',
    extends: [
      'eslint:recommended',
      'plugin:@typescript-eslint/recommended'
    ],
    plugins: ['@typescript-eslint'],
    rules: {
      // 添加你需要的规则
    }
  }
});