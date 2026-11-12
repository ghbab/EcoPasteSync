import { useMount, useUnmount } from "ahooks";
import { Button } from "antd";
import type { FC } from "react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  getPendingSyncPair,
  hideWindow,
  respondSyncPair,
  type SyncPairRequestPayload,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { WINDOW_LABEL } from "@/constants/windows";
import { useTauriListen } from "@/hooks/useTauriListen";
import { log } from "@/utils/log";

/** 待确认请求的轮询间隔：Rust 侧 120 秒过期，窗口据此在请求失效后自动退出。 */
const PENDING_REFRESH_INTERVAL_MS = 5000;

/**
 * 局域网同步配对请求窗口。
 *
 * 独立于剪贴板面板居中弹出：窗口由 Rust 在有请求时创建 / 显示，
 * 页面挂载后主动拉取当前请求（事件可能早于 WebView 加载而丢失）。
 */
const PairRequest: FC = () => {
  const { t } = useTranslation(["preferences", "common"]);
  const [request, setRequest] = useState<SyncPairRequestPayload | null>(null);
  const [responding, setResponding] = useState(false);
  const refreshTimerRef = useRef<number | null>(null);

  // 窗口创建晚于事件发送：挂载后补拉当前待确认请求。
  useMount(async () => {
    await refreshRequest();
  });

  // 定期回读：请求过期或在别处处理后关闭窗口，新请求则替换内容。
  useMount(() => {
    refreshTimerRef.current = window.setInterval(() => {
      void refreshRequest();
    }, PENDING_REFRESH_INTERVAL_MS);
  });

  useUnmount(() => {
    if (refreshTimerRef.current === null) {
      return;
    }

    window.clearInterval(refreshTimerRef.current);
    refreshTimerRef.current = null;
  });

  // 窗口存活期间又收到新请求时只在当前为空时展示：已有请求不能被顶替，
  // 否则用户核对 SAS 期间内容可能被换成另一台设备。
  useTauriListen<SyncPairRequestPayload>(
    TAURI_EVENT.SYNC_PAIR_REQUEST,
    (event) => {
      setRequest((current) => {
        return current ?? event.payload;
      });
    },
  );

  /**
   * 拉取当前待确认请求；没有有效请求时关闭窗口。
   */
  async function refreshRequest() {
    try {
      const pending = await getPendingSyncPair();

      if (pending === null) {
        await closeWindow();
        return;
      }

      setRequest(pending);
    } catch {
      // 命令层已统一提示；保留当前内容，等待下一轮刷新。
    }
  }

  /**
   * 应答当前请求；处理完后若还有其它设备在等待则继续展示，否则关闭窗口。
   */
  const handleRespond = async (accept: boolean) => {
    if (request === null || responding) {
      return;
    }

    setResponding(true);
    try {
      await respondSyncPair(request.deviceId, accept);
    } catch {
      // 请求可能已过期并被清理；命令层已提示，继续走刷新逻辑。
    } finally {
      setResponding(false);
    }

    await refreshRequest();
  };

  const handleAccept = async () => {
    await handleRespond(true);
  };

  const handleReject = async () => {
    await handleRespond(false);
  };

  if (request === null) {
    return (
      <div className="size-screen bg-transparent" data-tauri-drag-region />
    );
  }

  return (
    <div className="size-screen bg-transparent p-3">
      <div className="flex h-full flex-col overflow-hidden rounded-4 border border-ant-border-secondary bg-ant-elevated shadow-2xl">
        <div
          className="flex shrink-0 items-center justify-center px-4 pt-4 pb-2"
          data-tauri-drag-region
        >
          <span className="font-medium text-ant-text text-base">
            {t("schema.settings.sync.devices.pairRequestTitle")}
          </span>
        </div>

        <div className="flex min-h-0 flex-1 flex-col justify-center gap-2 px-6">
          <span className="break-words text-center text-sm">
            {t("schema.settings.sync.devices.pairRequestContent", {
              name: request.name,
              platform: resolvePlatformLabel(request.platform),
            })}
          </span>

          <span className="text-center text-ant-secondary text-xs">
            {t("schema.settings.sync.devices.pairRequestSasHint")}
          </span>

          <span className="text-center font-semibold text-lg tracking-widest">
            {request.sas}
          </span>
        </div>

        <div className="flex shrink-0 justify-end gap-2 px-4 py-3">
          <Button disabled={responding} onClick={handleReject}>
            {t("schema.settings.sync.devices.pairRequestReject")}
          </Button>

          <Button loading={responding} onClick={handleAccept} type="primary">
            {t("schema.settings.sync.devices.pairRequestAccept")}
          </Button>
        </div>
      </div>
    </div>
  );
};

/**
 * 关闭配对窗口：走统一隐藏命令，空闲后由生命周期策略销毁。
 */
const closeWindow = async () => {
  try {
    await hideWindow(WINDOW_LABEL.PAIR_REQUEST);
  } catch (error) {
    log.error("hide pair request window failed", error);
  }
};

/**
 * 平台展示名；Rust 只上报 macos / windows，未知值原样展示。
 */
function resolvePlatformLabel(platform: string) {
  if (platform === "macos") {
    return "macOS";
  }

  if (platform === "windows") {
    return "Windows";
  }

  return platform;
}

export default PairRequest;
