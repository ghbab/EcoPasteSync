/**
 * Tauri 窗口 label 常量。
 */
export const WINDOW_LABEL = {
  /**
   * 剪贴板窗口（剪贴板历史列表）。
   */
  CLIPBOARD: "clipboard",
  /**
   * 自定义右键菜单窗口（仅 Windows，绕开 muda `TrackPopupMenu` 抢焦点的问题）。
   */
  CONTEXT_MENU: "context-menu",
  /**
   * Custom context submenu window on Windows.
   */
  CONTEXT_SUBMENU: "context-submenu",
  /**
   * 局域网文件传输进度窗口（桌面居中置顶，参照系统复制对话框）。
   */
  FILE_TRANSFER: "file-transfer",
  /**
   * 首次启动引导窗口。
   */
  ONBOARDING: "onboarding",
  /**
   * 局域网同步配对请求窗口（桌面居中置顶）。
   */
  PAIR_REQUEST: "pair-request",
  /**
   * 偏好设置窗口。
   */
  PREFERENCE: "preference",
  /**
   * 剪贴板系统级预览 overlay 窗口。
   */
  PREVIEW: "clipboard-preview",
} as const;
