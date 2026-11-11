import { useMount } from "ahooks";
import { Button, Empty, Popconfirm, Spin, Switch, Tag, Typography } from "antd";
import type { FC, ReactNode } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type BlockedDevice,
  getSyncState,
  type NearbyDevice,
  type PairedDevice,
  refreshSyncDevices,
  removeSyncDevice,
  renameSyncDevice,
  requestSyncPair,
  type SyncPairResultPayload,
  type SyncState,
  setSyncDeviceBlocked,
  setSyncDevicePermissions,
} from "@/commands";
import CustomIconButton from "@/components/CustomIconButton";
import Tooltip from "@/components/Tooltip";
import { TAURI_EVENT } from "@/constants/events";
import { useTauriListen } from "@/hooks/useTauriListen";
import { cn } from "@/utils/cn";
import { getMessageApi } from "@/utils/feedback";

/**
 * 局域网同步设备管理：附近设备配对、已配对设备权限与备注、已忽略设备恢复。
 * 状态真相源在 Rust；本组件经 `sync://devices-updated` 事件回灌刷新。
 */
const SyncDevicesControl: FC = () => {
  const { t } = useTranslation(["preferences", "common"]);
  const [syncState, setSyncState] = useState<SyncState | null>(null);
  const [pairingDeviceId, setPairingDeviceId] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  useMount(async () => {
    try {
      const state = await getSyncState();

      setSyncState(state);
    } catch {
      // 命令层已统一 toast，这里只跳过首屏填充。
    }
  });

  useTauriListen<SyncState>(TAURI_EVENT.SYNC_DEVICES_UPDATED, (event) => {
    setSyncState(event.payload);
  });

  // 发起端在收到配对结果后结束按钮 loading 并提示成功 / 失败原因。
  useTauriListen<SyncPairResultPayload>(
    TAURI_EVENT.SYNC_PAIR_RESULT,
    (event) => {
      const { accepted, deviceId, reason } = event.payload;

      if (deviceId !== pairingDeviceId) return;

      setPairingDeviceId(null);

      if (accepted) {
        getMessageApi().success(t("schema.settings.sync.devices.pairSuccess"));

        return;
      }

      getMessageApi().error(
        t(
          `schema.settings.sync.devices.pairFailed.${resolvePairFailureKey(reason)}`,
        ),
      );
    },
  );

  const handleRefresh = async () => {
    setRefreshing(true);

    try {
      await refreshSyncDevices();
    } catch {
      // 命令层已统一 toast；列表由 devices-updated 事件回灌。
    } finally {
      setRefreshing(false);
    }
  };

  const handlePair = async (deviceId: string) => {
    setPairingDeviceId(deviceId);

    try {
      await requestSyncPair(deviceId);
    } catch {
      // 发起失败直接结束 loading；成功时等待 pair-result 事件。
      setPairingDeviceId(null);
    }
  };

  const handleRename = async (deviceId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) return;

    try {
      await renameSyncDevice(deviceId, trimmed);
    } catch {
      // 命令层已统一 toast；名称等待 devices-updated 事件回灌。
    }
  };

  const handleSendChange = async (deviceId: string, checked: boolean) => {
    try {
      await setSyncDevicePermissions(deviceId, checked, null);
    } catch {
      // 命令层已统一 toast；开关状态等待 devices-updated 事件回灌。
    }
  };

  const handleReceiveChange = async (deviceId: string, checked: boolean) => {
    try {
      await setSyncDevicePermissions(deviceId, null, checked);
    } catch {
      // 命令层已统一 toast；开关状态等待 devices-updated 事件回灌。
    }
  };

  const handleUnpair = async (deviceId: string) => {
    try {
      await removeSyncDevice(deviceId);
    } catch {
      // 命令层已统一 toast；列表等待 devices-updated 事件回灌。
    }
  };

  const handleUnblock = async (deviceId: string) => {
    try {
      await setSyncDeviceBlocked(deviceId, false);
    } catch {
      // 命令层已统一 toast；列表等待 devices-updated 事件回灌。
    }
  };

  if (syncState === null) {
    return (
      <div className="flex items-center justify-center py-10">
        <Spin />
      </div>
    );
  }

  if (!syncState.enabled) {
    return (
      <div className="px-4 py-6">
        <Empty
          description={t("schema.settings.sync.devices.disabledHint")}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  const pairedIds = new Set(
    syncState.paired.map((device) => {
      return device.deviceId;
    }),
  );
  const blockedIds = new Set(
    syncState.blocked.map((device) => {
      return device.deviceId;
    }),
  );
  const nearbyDevices = syncState.nearby.filter((device) => {
    return !pairedIds.has(device.deviceId) && !blockedIds.has(device.deviceId);
  });

  return (
    <div
      className="flex flex-col gap-2 py-2"
      data-preference-setting-id="sync.devices"
    >
      <div className="flex items-center justify-between gap-2 px-4">
        <span className="min-w-0 truncate text-ant-secondary text-xs">
          {t("schema.settings.sync.devices.localDeviceName", {
            name: syncState.deviceName,
          })}
        </span>

        <Tooltip title={t("schema.settings.sync.devices.refresh")}>
          <CustomIconButton
            aria-label={t("schema.settings.sync.devices.refresh")}
            icon={<i aria-hidden="true" className="i-lucide:refresh-cw" />}
            loading={refreshing}
            onClick={handleRefresh}
            size="small"
            type="text"
          />
        </Tooltip>
      </div>

      <DeviceGroup title={t("schema.settings.sync.devices.nearbyTitle")}>
        {nearbyDevices.length === 0 ? (
          <EmptyRow text={t("schema.settings.sync.devices.emptyNearby")} />
        ) : (
          nearbyDevices.map((device) => {
            return (
              <NearbyDeviceRow
                device={device}
                disabled={pairingDeviceId !== null}
                key={device.deviceId}
                loading={pairingDeviceId === device.deviceId}
                onPair={handlePair}
              />
            );
          })
        )}
      </DeviceGroup>

      <DeviceGroup title={t("schema.settings.sync.devices.pairedTitle")}>
        {syncState.paired.length === 0 ? (
          <EmptyRow text={t("schema.settings.sync.devices.emptyPaired")} />
        ) : (
          syncState.paired.map((device) => {
            return (
              <PairedDeviceRow
                device={device}
                key={device.deviceId}
                onReceiveChange={handleReceiveChange}
                onRename={handleRename}
                onSendChange={handleSendChange}
                onUnpair={handleUnpair}
              />
            );
          })
        )}
      </DeviceGroup>

      <DeviceGroup title={t("schema.settings.sync.devices.blockedTitle")}>
        {syncState.blocked.length === 0 ? (
          <EmptyRow text={t("schema.settings.sync.devices.emptyBlocked")} />
        ) : (
          syncState.blocked.map((device) => {
            return (
              <BlockedDeviceRow
                device={device}
                key={device.deviceId}
                onUnblock={handleUnblock}
              />
            );
          })
        )}
      </DeviceGroup>
    </div>
  );
};

interface DeviceGroupProps {
  children: ReactNode;
  title: string;
}

/**
 * 设备列表分组：小节标题 + 分隔线包裹的条目区。
 */
const DeviceGroup: FC<DeviceGroupProps> = (props) => {
  const { children, title } = props;

  return (
    <section className="flex flex-col">
      <h3 className="m-0 px-4 pt-2 pb-1 font-medium text-ant-secondary text-xs">
        {title}
      </h3>

      <div className="border-ant-split border-t">{children}</div>
    </section>
  );
};

/**
 * 分组空列表占位行。
 */
const EmptyRow: FC<{ text: string }> = (props) => {
  const { text } = props;

  return <div className="px-4 py-3 text-ant-tertiary text-xs">{text}</div>;
};

/**
 * 平台展示 Tag；Rust 只上报 macos / windows，未知值原样展示。
 */
const PlatformTag: FC<{ platform: string }> = (props) => {
  const { platform } = props;
  const label = resolvePlatformLabel(platform);

  return <Tag className="m-0">{label}</Tag>;
};

function resolvePlatformLabel(platform: string) {
  if (platform === "macos") {
    return "macOS";
  }

  if (platform === "windows") {
    return "Windows";
  }

  return platform;
}

/**
 * 把配对失败原因归一到文案表内的 key，未知原因按内部错误处理。
 */
function resolvePairFailureKey(reason: string | null) {
  switch (reason) {
    case "timeout":
    case "pair-rejected":
    case "network-error":
    case "device-offline":
    case "internal-error":
      return reason;
    default:
      return "internal-error";
  }
}

interface NearbyDeviceRowProps {
  device: NearbyDevice;
  disabled: boolean;
  loading: boolean;
  onPair: (deviceId: string) => Promise<void>;
}

/**
 * 附近设备条目：名称 + 平台 + 配对按钮；配对期间其余按钮禁用。
 */
const NearbyDeviceRow: FC<NearbyDeviceRowProps> = (props) => {
  const { t } = useTranslation("preferences");
  const { device, disabled, loading, onPair } = props;

  const handlePair = async () => {
    await onPair(device.deviceId);
  };

  return (
    <div className="flex items-center gap-2 border-ant-split border-b px-4 py-2 last:border-b-0">
      <span className="min-w-0 flex-1 truncate text-ant-text text-sm">
        {device.name}
      </span>

      <PlatformTag platform={device.platform} />

      <Button
        disabled={disabled}
        loading={loading}
        onClick={handlePair}
        size="small"
        type="primary"
      >
        {t("schema.settings.sync.devices.pair")}
      </Button>
    </div>
  );
};

interface PairedDeviceRowProps {
  device: PairedDevice;
  onRename: (deviceId: string, name: string) => Promise<void>;
  onSendChange: (deviceId: string, checked: boolean) => Promise<void>;
  onReceiveChange: (deviceId: string, checked: boolean) => Promise<void>;
  onUnpair: (deviceId: string) => Promise<void>;
}

/**
 * 已配对设备条目：可编辑名称、平台、在线状态、发送 / 接收权限与解除配对。
 */
const PairedDeviceRow: FC<PairedDeviceRowProps> = (props) => {
  const { t } = useTranslation(["preferences", "common"]);
  const { device, onRename, onSendChange, onReceiveChange, onUnpair } = props;

  const handleRename = (value: string) => {
    void onRename(device.deviceId, value);
  };

  const handleSendChange = async (checked: boolean) => {
    await onSendChange(device.deviceId, checked);
  };

  const handleReceiveChange = async (checked: boolean) => {
    await onReceiveChange(device.deviceId, checked);
  };

  const handleUnpair = async () => {
    await onUnpair(device.deviceId);
  };

  return (
    <div className="flex items-center gap-3 border-ant-split border-b px-4 py-2 last:border-b-0">
      <Typography.Text
        className="min-w-0 flex-1 truncate text-sm"
        editable={{
          onChange: handleRename,
          tooltip: t("schema.settings.sync.devices.rename"),
        }}
      >
        {device.name}
      </Typography.Text>

      <PlatformTag platform={device.platform} />

      <span className="flex shrink-0 items-center gap-1">
        <span
          className={cn("size-1.5 rounded-full", {
            "bg-ant-disabled": !device.online,
            "bg-ant-success": device.online,
          })}
        />
        <span className="text-ant-secondary text-xs">
          {device.online
            ? t("schema.settings.sync.devices.online")
            : t("schema.settings.sync.devices.offline")}
        </span>
      </span>

      <span className="flex shrink-0 items-center gap-1">
        <span className="text-ant-secondary text-xs">
          {t("schema.settings.sync.devices.send")}
        </span>
        <Switch
          checked={device.allowSend}
          onChange={handleSendChange}
          size="small"
        />
      </span>

      <span className="flex shrink-0 items-center gap-1">
        <span className="text-ant-secondary text-xs">
          {t("schema.settings.sync.devices.receive")}
        </span>
        <Switch
          checked={device.allowReceive}
          onChange={handleReceiveChange}
          size="small"
        />
      </span>

      <Popconfirm
        cancelText={t("common:actions.cancel")}
        okButtonProps={{ danger: true }}
        okText={t("schema.settings.sync.devices.unpair")}
        onConfirm={handleUnpair}
        title={t("schema.settings.sync.devices.unpairConfirm")}
      >
        <Button danger size="small" type="text">
          {t("schema.settings.sync.devices.unpair")}
        </Button>
      </Popconfirm>
    </div>
  );
};

interface BlockedDeviceRowProps {
  device: BlockedDevice;
  onUnblock: (deviceId: string) => Promise<void>;
}

/**
 * 已忽略设备条目：名称缺失时退回设备 id 前 8 位，支持取消忽略。
 */
const BlockedDeviceRow: FC<BlockedDeviceRowProps> = (props) => {
  const { t } = useTranslation("preferences");
  const { device, onUnblock } = props;

  const handleUnblock = async () => {
    await onUnblock(device.deviceId);
  };

  return (
    <div className="flex items-center gap-2 border-ant-split border-b px-4 py-2 last:border-b-0">
      <span className="min-w-0 flex-1 truncate text-ant-text text-sm">
        {device.name ?? device.deviceId.slice(0, 8)}
      </span>

      {device.platform !== null ? (
        <PlatformTag platform={device.platform} />
      ) : null}

      <Button onClick={handleUnblock} size="small">
        {t("schema.settings.sync.devices.unblock")}
      </Button>
    </div>
  );
};

export default SyncDevicesControl;
