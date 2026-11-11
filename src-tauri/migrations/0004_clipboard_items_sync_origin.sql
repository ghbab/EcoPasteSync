-- 标记同步入库条目的来源设备。
-- 文件按需拉取只授权本机产生的 files 条目路径，避免已配对设备先推送伪造路径、
-- 再借拉取接口读取本机任意文件。本机复制产生的条目该列为 NULL。

ALTER TABLE clipboard_items ADD COLUMN sync_source_device_id TEXT;
