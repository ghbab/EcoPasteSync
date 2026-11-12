-- 远端文件条目的可拉取指纹与本地缓存路径。
-- files_json：发送端推送的每文件指纹（path / isDir / size / mtimeMs / headHash / hash），
--   接收端据此在粘贴时按需分块拉取并校验完整性。
-- local_paths_json：上一次拉取成功后的本地缓存路径列表，重复粘贴时直接复用。

ALTER TABLE sync_file_states ADD COLUMN files_json TEXT;
ALTER TABLE sync_file_states ADD COLUMN local_paths_json TEXT;
