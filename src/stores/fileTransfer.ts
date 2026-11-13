import { proxy } from "valtio";

export interface FileTransferProgress {
  transferredBytes: number;
  totalBytes: number;
}

interface FileTransferState {
  /** itemId → 当前传输进度；传输结束后删除。 */
  items: Record<string, FileTransferProgress>;
}

/**
 * 远端文件按需传输的实时进度（非持久化）。
 *
 * Rust 在粘贴前拉取文件时通过 `sync://file-transfer-progress` 持续上报，
 * FilesCard 读取后渲染进度条。
 */
export const fileTransferState = proxy<FileTransferState>({ items: {} });

/**
 * 更新某条目的传输进度。
 */
export const setFileTransferProgress = (
  itemId: string,
  progress: FileTransferProgress,
) => {
  fileTransferState.items[itemId] = progress;
};

/**
 * 传输结束（成功或失败）后清除进度。
 */
export const clearFileTransferProgress = (itemId: string) => {
  delete fileTransferState.items[itemId];
};
