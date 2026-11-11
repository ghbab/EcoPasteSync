import type { FC } from "react";
import { useTranslation } from "react-i18next";
import {
  respondSyncPair,
  type SyncPairRequestPayload,
} from "@/commands";
import { TAURI_EVENT } from "@/constants/events";
import { useTauriListen } from "@/hooks/useTauriListen";
import { getModalApi } from "@/utils/feedback";

/**
 * 剪贴板主窗口常驻的同步配对请求监听器：
 * 收到 `sync://pair-request` 时弹出确认框，用户选择后回传 Rust。
 * 只随剪贴板窗口挂载一次，偏好窗口不重复挂载。
 */
const SyncPairRequestListener: FC = () => {
  const { t } = useTranslation(["preferences", "common"]);

  const handlePairRequest = (payload: SyncPairRequestPayload) => {
    getModalApi().confirm({
      cancelText: t("common:actions.cancel"),
      centered: true,
      content: t("schema.settings.sync.devices.pairRequestContent", {
        name: payload.name,
        platform: resolvePlatformLabel(payload.platform),
      }),
      okText: t("schema.settings.sync.devices.pairRequestAccept"),
      onCancel: () => {
        void respondQuietly(payload.deviceId, false);
      },
      onOk: async () => {
        await respondQuietly(payload.deviceId, true);
      },
      title: t("schema.settings.sync.devices.pairRequestTitle"),
    });
  };

  useTauriListen<SyncPairRequestPayload>(
    TAURI_EVENT.SYNC_PAIR_REQUEST,
    (event) => {
      handlePairRequest(event.payload);
    },
  );

  return null;
};

/**
 * 应答配对请求；失败时命令层已统一 toast，这里吞掉保证弹窗正常关闭。
 */
async function respondQuietly(deviceId: string, accept: boolean) {
  try {
    await respondSyncPair(deviceId, accept);
  } catch {
    // 忽略：命令层已记录日志并提示。
  }
}

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

export default SyncPairRequestListener;
