-- 局域网同步：已配对（互信）设备与黑名单。
-- shared_key 为配对时双方各自派生的一致密钥（hex），用于请求认证与报文加密。

CREATE TABLE sync_devices (
    device_id      TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    platform       TEXT NOT NULL,
    shared_key     TEXT NOT NULL,
    allow_send     INTEGER NOT NULL DEFAULT 1,
    allow_receive  INTEGER NOT NULL DEFAULT 1,
    last_seen_at   TEXT,
    last_address   TEXT,
    last_pull_at   TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE TABLE sync_blacklist (
    device_id   TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
