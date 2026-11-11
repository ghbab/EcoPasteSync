-- 同步文件条目的传输状态：仅用于「超过本体传输阈值、只同步路径」的条目。
-- 接收端据此决定是否按需拉取、以及从哪一块续传。
-- chunk_bitmap 为每个文件已完成块的位图（hex，按文件顺序以 ';' 分隔），
-- 传输中断后重启仍可续传，无需重头拉取。

CREATE TABLE sync_file_states (
    item_id            TEXT PRIMARY KEY,
    device_id          TEXT NOT NULL,
    state              TEXT NOT NULL,
    total_bytes        INTEGER NOT NULL DEFAULT 0,
    transferred_bytes  INTEGER NOT NULL DEFAULT 0,
    chunk_size         INTEGER NOT NULL,
    file_count         INTEGER NOT NULL DEFAULT 0,
    chunk_bitmap       TEXT NOT NULL DEFAULT '',
    cache_dir          TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE INDEX idx_sync_file_states_device ON sync_file_states (device_id);
CREATE INDEX idx_sync_file_states_state ON sync_file_states (state);
