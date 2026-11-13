-- 文件传输任务日志（journal）：用于崩溃恢复与断点续传。
--
-- 与旧的 sync_file_states 不同，这里记录的是「一次具体的落盘任务」：
-- 目标目录、每个文件的临时文件路径、已完成字节数。应用异常退出后，
-- 启动时按这里记录的临时路径删除残留的 .EcoPasteSync.tmp 文件，
-- 绝不会误删用户的原文件（只删后缀固定且路径来自本表的临时文件）。
--
-- files_json 结构：
-- [{ "source": 远端绝对路径, "tmp": 本地临时路径, "finalPath": 最终路径,
--    "size": 字节数, "done": 已写入字节数, "isDir": 是否目录 }]
--
-- status 取值：queued / running / paused / failed / done / cancelled。
-- done 与 cancelled 的行在任务结束后直接删除，只有未完成的行会留存到下次启动。

CREATE TABLE sync_transfer_jobs (
    job_id      TEXT PRIMARY KEY,
    item_id     TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    target_dir  TEXT NOT NULL,
    status      TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    done_bytes  INTEGER NOT NULL DEFAULT 0,
    files_json  TEXT NOT NULL DEFAULT '[]',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX idx_sync_transfer_jobs_status ON sync_transfer_jobs (status);

-- 本地副本指纹快照：判断「上次落盘的文件是否被用户改过」。
-- 结构与 files_json 类似，记录落盘后的本地路径与 size / mtime / hash。
ALTER TABLE sync_file_states ADD COLUMN local_manifest_json TEXT;
