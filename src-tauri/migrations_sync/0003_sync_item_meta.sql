-- 同步入库条目的来源设备。
-- 不进主库 clipboard_items 表，避免修改上游 schema；查询时按 item_id 关联。
-- 文件按需拉取只授权本机产生的 files 条目路径，避免已配对设备先推送伪造路径、
-- 再借拉取接口读取本机任意文件。本机复制产生的条目在该表没有记录。

CREATE TABLE sync_item_meta (
    item_id          TEXT PRIMARY KEY,
    source_device_id TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE INDEX idx_sync_item_meta_source_device ON sync_item_meta (source_device_id);
