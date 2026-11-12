use crate::i18n::keys::CommandKey as Key;

/// 返回简体中文 Tauri 命令错误根因文案。
pub fn label(key: Key) -> &'static str {
    match key {
        Key::DragSourceFilesMissing => "拖拽源文件已不存在",
        Key::DragImageMissing => "图片文件已不存在",
        Key::DragTextEmpty => "文本内容为空",
        Key::ExternalUrlUnsupported => "只能打开 http 或 https 开头的链接",
        Key::SyncDisabled => "同步未开启",
        Key::SyncFileUnavailable => "该远端文件无法传输（可能超出上限或是目录）",
        Key::SyncFileTransferFailed => "远端文件传输失败",
        Key::SyncFileTransferBusy => "该文件正在传输，请稍候",
    }
}
