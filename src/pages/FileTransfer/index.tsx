import { getCurrentWindow } from "@tauri-apps/api/window";
import { open } from "@tauri-apps/plugin-dialog";
import { useMount, useUnmount } from "ahooks";
import { Button, Progress, Switch } from "antd";
import type { FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  cancelFileTransfer,
  copyFileTransferPaths,
  type FileTransferSnapshot,
  getLatestFileTransfer,
  hideWindow,
  pauseFileTransfer,
  resolveFileTransferConflicts,
  resumeFileTransfer,
  retryFileTransferToDir,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { isMac } from "@/utils/is";
import { log } from "@/utils/log";

/** 完成后停留时间：先让用户看到「已完成」，再自动关闭窗口。 */
const AUTO_CLOSE_DELAY_MS = 1500;

/**
 * 局域网文件传输进度窗口。
 *
 * 由 Rust 在粘贴触发传输时创建并显示；页面只负责展示快照、发控制指令。
 * 首帧主动拉取最近任务，避免窗口创建晚于第一个进度事件。
 */
const FileTransfer: FC = () => {
  const { t } = useTranslation("commands");
  const [snapshot, setSnapshot] = useState<FileTransferSnapshot | null>(null);
  const [selectedOverwrite, setSelectedOverwrite] = useState<string[]>([]);
  const closeTimerRef = useRef<number | null>(null);

  useMount(async () => {
    try {
      const latest = await getLatestFileTransfer();
      if (latest !== null) {
        // 全部冲突被跳过的任务无需展示进度，直接收起窗口。
        if (latest.status === "skipped") {
          void closeWindow();
          return;
        }
        setSnapshot(latest);
      }
    } catch {
      // 命令层已统一提示；没有任务时窗口保持空白背景。
    }
  });

  useUnmount(() => {
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  });

  useTauriListen<FileTransferSnapshot>(
    TAURI_EVENT.SYNC_FILE_PROGRESS,
    (event) => {
      const next = event.payload;

      // 全部冲突被跳过、没有实际待传文件时，窗口立即收起，不显示进度条。
      if (next.status === "skipped") {
        void closeWindow();
        return;
      }

      setSnapshot(next);

      // 同一任务结束后自动收起窗口；失败态保留，等待用户处理。
      if (next.done || next.status === "cancelled") {
        scheduleClose();
      }
    },
  );

  /** 结束后延迟关闭窗口。 */
  function scheduleClose() {
    if (closeTimerRef.current !== null) {
      return;
    }
    closeTimerRef.current = window.setTimeout(() => {
      closeTimerRef.current = null;
      void closeWindow();
    }, AUTO_CLOSE_DELAY_MS);
  }

  /** 隐藏窗口（由生命周期策略在空闲后销毁）。 */
  async function closeWindow() {
    try {
      await hideWindow(WINDOW_LABEL.FILE_TRANSFER);
    } catch (error) {
      log.error("hide file transfer window failed", error);
    }
  }

  const handlePause = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await pauseFileTransfer(snapshot.jobId);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleResume = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await resumeFileTransfer(snapshot.jobId);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleCancel = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await cancelFileTransfer(snapshot.jobId);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleResolve = async (policy: string) => {
    if (snapshot === null) {
      return;
    }
    try {
      await resolveFileTransferConflicts(snapshot.jobId, policy);
      setSelectedOverwrite([]);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleCustomResolve = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await resolveFileTransferConflicts(
        snapshot.jobId,
        "custom",
        selectedOverwrite,
      );
      setSelectedOverwrite([]);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleRetry = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await retryFileTransferToDir(snapshot.jobId, snapshot.targetDir);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleChooseDir = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      const directory = await open({ directory: true, multiple: false });
      if (typeof directory !== "string") {
        return;
      }
      await retryFileTransferToDir(snapshot.jobId, directory);
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleCopyPaths = async () => {
    if (snapshot === null) {
      return;
    }
    try {
      await copyFileTransferPaths(snapshot.jobId);
      await closeWindow();
    } catch {
      // 命令层已统一提示。
    }
  };

  const handleMinimize = async () => {
    try {
      await getCurrentWindow().minimize();
    } catch (error) {
      log.error("minimize file transfer window failed", error);
    }
  };

  const toggleOverwrite = (path: string, checked: boolean) => {
    setSelectedOverwrite((current) => {
      if (checked) {
        return current.includes(path) ? current : [...current, path];
      }
      return current.filter((item) => item !== path);
    });
  };

  if (snapshot === null) {
    return (
      <div className="size-screen bg-transparent" data-tauri-drag-region />
    );
  }

  const statusText = resolveStatusText(snapshot, t);
  const percent = resolvePercent(snapshot);
  const isPreparing = snapshot.status === "queued" && snapshot.fileCount === 0;
  const isRunning = snapshot.status === "running";
  const isPaused = snapshot.status === "paused";
  const isConflicts = snapshot.status === "conflicts";
  const isFailed = snapshot.status === "failed";

  // 窗口本身是 transparent + decorations(false)：
  // macOS 用透明留边展示系统阴影与圆角卡片；Windows 上留边会露出一圈透明区域，
  // 看起来像多了一道透明边框，因此改为不透明背景铺满整个窗口。
  return (
    <div
      className={
        isMac ? "size-screen bg-transparent p-3" : "size-screen bg-ant-elevated"
      }
    >
      <div
        className={
          isMac
            ? "flex h-full flex-col overflow-hidden rounded-4 border border-ant-border-secondary bg-ant-elevated shadow-2xl"
            : "flex h-full flex-col overflow-hidden"
        }
      >
        {/* 标题栏：整行可拖动窗口 */}
        <div
          className="flex shrink-0 items-center justify-between px-4 pt-3 pb-2"
          data-tauri-drag-region
        >
          <span className="font-medium text-ant-text text-sm">
            {t("fileTransfer.title")}
          </span>

          <button
            aria-label="minimize"
            className="cursor-pointer border-none bg-transparent px-2 text-ant-secondary text-base leading-none hover:text-ant-text"
            onClick={handleMinimize}
            type="button"
          >
            —
          </button>
        </div>

        <div className="flex min-h-0 flex-1 flex-col gap-2 px-4 pb-2">
          <span className="truncate text-ant-text text-xs">{statusText}</span>

          {isConflicts ? (
            <div className="flex min-h-0 flex-1 flex-col gap-2 overflow-hidden">
              <span className="text-ant-secondary text-xs">
                {t("fileTransfer.conflictsHint")}
              </span>

              <span className="text-ant-secondary text-xs">
                {t("fileTransfer.conflictsToggleHint")}
              </span>

              {/* 背景必须用已注册的 antd 颜色 token：bg-ant-base 在 antd v6 下没有对应变量，
                  容器会变成透明并透出窗口后面的内容（看起来是一块黑色）。 */}
              <div className="min-h-0 flex-1 overflow-y-auto rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-2">
                {snapshot.conflicts.map((path) => (
                  <div
                    className="flex items-center justify-between gap-2 py-1"
                    key={path}
                  >
                    <span
                      className="truncate text-ant-secondary text-xs"
                      title={path}
                    >
                      {path}
                    </span>
                    <Switch
                      checked={selectedOverwrite.includes(path)}
                      onChange={(checked) => toggleOverwrite(path, checked)}
                      size="small"
                    />
                  </div>
                ))}
              </div>

              <div className="flex shrink-0 justify-end gap-2">
                <Button onClick={handleCustomResolve} size="small">
                  {t("fileTransfer.custom")}
                </Button>
                <Button onClick={() => handleResolve("skip")} size="small">
                  {t("fileTransfer.skip")}
                </Button>
                <Button onClick={() => handleResolve("keepBoth")} size="small">
                  {t("fileTransfer.keepBoth")}
                </Button>
                <Button
                  onClick={() => handleResolve("overwrite")}
                  size="small"
                  type="primary"
                >
                  {t("fileTransfer.overwrite")}
                </Button>
              </div>
            </div>
          ) : (
            <>
              <Progress
                percent={percent}
                showInfo={false}
                size="small"
                status={isFailed ? "exception" : "normal"}
              />

              <div className="flex justify-between text-ant-secondary text-xs">
                <span>
                  {formatBytes(snapshot.transferredBytes)} /{" "}
                  {formatBytes(snapshot.totalBytes)}
                </span>
                <span>
                  {isPreparing
                    ? t("fileTransfer.preparing")
                    : formatSpeed(snapshot, t)}
                </span>
              </div>

              {isFailed && snapshot.error !== null ? (
                <span
                  className="line-clamp-2 text-ant-error text-xs"
                  title={snapshot.error}
                >
                  {snapshot.error}
                </span>
              ) : null}

              {snapshot.done ? (
                <span className="text-ant-text text-xs">
                  {snapshot.status === "cancelled"
                    ? t("fileTransfer.cancelled")
                    : t("fileTransfer.completed")}
                </span>
              ) : null}

              <div className="mt-auto flex shrink-0 justify-end gap-2">
                {isFailed ? (
                  <>
                    <Button onClick={handleCopyPaths} size="small">
                      {t("fileTransfer.copyPathsOnly")}
                    </Button>
                    <Button onClick={handleChooseDir} size="small">
                      {t("fileTransfer.chooseDir")}
                    </Button>
                    <Button onClick={handleRetry} size="small" type="primary">
                      {t("fileTransfer.retry")}
                    </Button>
                  </>
                ) : (
                  <>
                    <Button onClick={handleCancel} size="small">
                      {t("fileTransfer.cancel")}
                    </Button>
                    {isPaused ? (
                      <Button
                        onClick={handleResume}
                        size="small"
                        type="primary"
                      >
                        {t("fileTransfer.resume")}
                      </Button>
                    ) : (
                      <Button
                        disabled={!isRunning && !isPreparing}
                        onClick={handlePause}
                        size="small"
                      >
                        {t("fileTransfer.pause")}
                      </Button>
                    )}
                  </>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

/**
 * 顶部状态文案：优先展示当前文件名与序号，冲突 / 失败态展示提示语。
 */
function resolveStatusText(
  snapshot: FileTransferSnapshot,
  t: (key: string, options?: Record<string, unknown>) => string,
) {
  if (snapshot.status === "conflicts") {
    return t("fileTransfer.conflictsTitle", {
      count: snapshot.conflicts.length,
    });
  }
  if (snapshot.status === "failed") {
    return t("fileTransfer.failed");
  }
  if (snapshot.fileName !== null) {
    return `${snapshot.fileIndex}/${snapshot.fileCount} ${snapshot.fileName}`;
  }
  return t("fileTransfer.preparing");
}

/** 进度百分比：总字节为 0 时按 0 处理，避免除零。 */
function resolvePercent(snapshot: FileTransferSnapshot) {
  if (snapshot.totalBytes <= 0) {
    return 0;
  }
  const percent = Math.floor(
    (snapshot.transferredBytes / snapshot.totalBytes) * 100,
  );
  return Math.max(0, Math.min(100, percent));
}

/** 速度 + 剩余时间文案。 */
function formatSpeed(
  snapshot: FileTransferSnapshot,
  t: (key: string, options?: Record<string, unknown>) => string,
) {
  if (snapshot.speedBps <= 0) {
    return "";
  }
  const speed = t("fileTransfer.speedUnit", {
    speed: `${formatBytes(snapshot.speedBps)}`,
  });
  const remainingBytes = Math.max(
    0,
    snapshot.totalBytes - snapshot.transferredBytes,
  );
  const remainingSeconds = Math.ceil(remainingBytes / snapshot.speedBps);

  return `${speed} · ${t("fileTransfer.remaining", {
    time: formatDuration(remainingSeconds),
  })}`;
}

/** 剩余时长：只展示到分钟，够用且不跳动。 */
function formatDuration(seconds: number) {
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m`;
}

/** 字节数转可读文本（1024 进制）。 */
function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  const digits = value >= 100 || unitIndex === 0 ? 0 : 1;
  return `${value.toFixed(digits)} ${units[unitIndex]}`;
}

export default FileTransfer;
