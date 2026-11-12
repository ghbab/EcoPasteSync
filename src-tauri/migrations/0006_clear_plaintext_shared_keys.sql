-- 共享密钥已迁移到系统钥匙串：清空历史版本写入的明文列，
-- 避免数据库文件里残留可直接解密同步流量的旧密钥。
UPDATE sync_devices SET shared_key = '';
