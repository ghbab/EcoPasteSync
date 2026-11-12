import packageJson from "../../package.json";

/**
 * 对外展示的应用版本号：上游三段版本 + 本项目发布序号。
 *
 * Tauri 打包要求版本是三段 semver，第四位只在展示与发布 tag 中使用；
 * nightly 等带后缀的构建版本不再追加发布序号。
 */
export const APP_VERSION = /^\d+\.\d+\.\d+$/.test(packageJson.version)
  ? `${packageJson.version}.${packageJson.ecopasteRelease}`
  : packageJson.version;
