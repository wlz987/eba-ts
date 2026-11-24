import js from "@eslint/js";
import tseslint from "typescript-eslint";
import globals from "globals";

// 工业化基线：
// 1. eslint:recommended —— 通用正确性规则（未用变量、比较陷阱、重赋值等）；
// 2. typescript-eslint recommended —— TS 卫生规则；
// 3. 类型感知：no-floating-promises 在源码里是 error（异步运行时的核心纪律）；
//    no-non-null-assertion 保持 warn —— 堆/环/队列热路径靠 `!` 配合
//    noUncheckedIndexedAccess 省掉每次读取的分支，逐处消除会稀释主逻辑。
export default tseslint.config(
  {
    ignores: ["dist/**", "node_modules/**", "scripts/rewrite-aliases.mjs"],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    languageOptions: {
      parserOptions: {
        projectService: {
          allowDefaultProject: ["*.mjs", "test/*.mjs", "scripts/*.mjs"],
        },
        ecmaVersion: 2024,
      },
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/no-non-null-assertion": "warn",
      "no-console": "error",
      "no-debugger": "error",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-require-imports": "error",
    },
  },
  {
    // 纯 JS 的脚本/测试文件：不进 TS projectService（它的默认项目最多 8 个
    // 文件，测试数量增长后会报错），因此只跑无需类型信息的规则。
    files: ["scripts/**/*.mjs", "test/**/*.mjs"],
    languageOptions: {
      globals: { ...globals.node },
      parserOptions: {
        projectService: false,
        ecmaVersion: 2024,
      },
    },
    rules: {
      "no-console": "off",
      // node:test's async `test()` calls return promises the runner owns.
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
    },
  },
);
