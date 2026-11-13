pub mod error;
pub mod paths;
pub mod prevent_default;
pub mod version;
#[cfg(target_os = "windows")]
pub mod windows_args;

pub use error::{AppError, Result};
