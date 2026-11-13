import { Progress } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import { useSnapshot } from "valtio";
import AssetImage from "@/components/AssetImage";
import Highlight from "@/components/Highlight";
import Tooltip from "@/components/Tooltip";
import { clipboardViewState } from "@/stores/clipboardView";
import { fileTransferState } from "@/stores/fileTransfer";
import type { ClipboardItem, FileEntry } from "@/types/clipboard";
import { cn } from "@/utils/cn";
import ImageCard from "./ImageCard";

/**
 * 文件类卡片：`fileEntries` 与 `filesPreviewKind` 均由 Rust 命令层预处理，
 * 前端只按 kind 分支渲染：`imagePreview` 走单图预览，`list` 走文件列表。
 *
 * - 本机路径已删除（`exists = false`）才划删除线；
 * - 远端同步条目（`remote = true`）保留原文件名并加来源标记；
 * - 粘贴远端文件时 Rust 按需传输，进度经 `fileTransferState` 驱动底部进度条。
 */
const FilesCard: FC<ClipboardItem> = (props) => {
  const entries = props.fileEntries ?? [];
  const { keyword } = useSnapshot(clipboardViewState);
  const transfer = useSnapshot(fileTransferState).items[props.id];

  if (props.filesPreviewKind === "imagePreview") {
    const [first] = entries;
    return <ImageCard {...props} imageThumbnailPath={first?.path} />;
  }

  const percent =
    transfer && transfer.totalBytes > 0
      ? Math.min(
          100,
          Math.round((transfer.transferredBytes / transfer.totalBytes) * 100),
        )
      : 0;

  return (
    <div className="flex flex-col gap-1">
      {entries.map((entry) => {
        return <FileRow entry={entry} key={entry.path} keyword={keyword} />;
      })}

      {transfer ? (
        <Progress
          percent={percent}
          showInfo={false}
          size="small"
          status="active"
        />
      ) : null}
    </div>
  );
};

interface FileRowProps {
  entry: FileEntry;
  keyword: string;
}

const FileRow: FC<FileRowProps> = (props) => {
  const { entry, keyword } = props;
  const { t } = useTranslation("clipboard");

  return (
    <div className="flex items-center gap-1 truncate" title={entry.path}>
      <AssetImage className="size-5" src={entry.iconPath} />

      <Highlight
        className={cn("truncate", {
          "line-through": !entry.exists && !entry.remote,
        })}
        keyword={keyword}
        text={entry.name}
      />

      {entry.remote ? (
        <Tooltip title={t("files.remoteSource")}>
          <i
            aria-hidden="true"
            className="i-lucide:cloud-download size-3 shrink-0 text-ant-tertiary"
          />
        </Tooltip>
      ) : null}
    </div>
  );
};

export default FilesCard;
