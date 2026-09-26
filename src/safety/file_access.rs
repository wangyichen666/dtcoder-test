//! Unix 文件能力：授权后的操作只使用已经打开的目录/文件句柄。
use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use super::FileAccessIntent;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
const MAX_FILE_READ_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone, Copy)]
struct Identity {
    dev: u64,
    ino: u64,
}

impl Identity {
    fn of(file: &File) -> Result<Self> {
        let meta = file.metadata()?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }

    fn matches(self, other: Self) -> bool {
        self.dev == other.dev && self.ino == other.ino
    }
}

struct Version {
    identity: Identity,
    sha256: [u8; 32],
}

/// 只能由 SafetyPolicy 创建。持有父目录和原文件身份，防止使用原始路径重新打开。
pub struct AuthorizedPath {
    path: PathBuf,
    parent: File,
    parent_identity: Identity,
    basename: CString,
    intent: FileAccessIntent,
    original: Option<File>,
    version: Option<Version>,
}

impl AuthorizedPath {
    pub(super) fn open(path: PathBuf, intent: FileAccessIntent) -> Result<Self> {
        let parent_path = path.parent().context("文件路径没有父目录")?;
        let basename = c_name(path.file_name().context("文件路径没有文件名")?)?;
        let parent = open_directory_tree(parent_path, matches!(intent, FileAccessIntent::Write))?;
        let parent_identity = Identity::of(&parent)?;
        let original = match open_regular_at(parent.as_raw_fd(), &basename)? {
            Some(file) => Some(file),
            None if matches!(intent, FileAccessIntent::Write) => None,
            None => bail!("文件不存在: {}", path.display()),
        };
        if !matches!(intent, FileAccessIntent::Read)
            && original
                .as_ref()
                .is_some_and(|file| file.metadata().is_ok_and(|meta| meta.nlink() > 1))
        {
            bail!("拒绝写入硬链接目标: {}", path.display());
        }
        let version = original.as_ref().map(version_of).transpose()?;
        Ok(Self {
            path,
            parent,
            parent_identity,
            basename,
            intent,
            original,
            version,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_bytes(&self) -> Result<Vec<u8>> {
        let file = self.original.as_ref().context("授权文件不存在")?;
        if file.metadata()?.len() > MAX_FILE_READ_BYTES {
            bail!("文件超过 32 MiB 读取上限");
        }
        let mut reader = file.try_clone()?;
        reader.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        reader
            .take(MAX_FILE_READ_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_FILE_READ_BYTES {
            bail!("文件超过 32 MiB 读取上限");
        }
        Ok(bytes)
    }

    pub fn read_to_string(&self) -> Result<String> {
        String::from_utf8(self.read_bytes()?).context("文件不是 UTF-8")
    }

    pub fn size(&self) -> Result<u64> {
        Ok(self
            .original
            .as_ref()
            .context("授权文件不存在")?
            .metadata()?
            .len())
    }

    pub fn atomic_write(&self, bytes: &[u8]) -> Result<()> {
        if matches!(self.intent, FileAccessIntent::Read) {
            bail!("只读能力不允许写入");
        }
        self.validate_current()?;
        let temp_name = CString::new(format!(
            ".agent-write-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ))?;
        let fd = unsafe {
            libc::openat(
                self.parent.as_raw_fd(),
                temp_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("创建临时文件失败");
        }
        let mut temp = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> Result<()> {
            if let Some(original) = &self.original {
                let mode = original.metadata()?.permissions().mode() & 0o7777;
                if unsafe { libc::fchmod(temp.as_raw_fd(), mode as libc::mode_t) } < 0 {
                    return Err(std::io::Error::last_os_error()).context("保留目标文件权限失败");
                }
            }
            temp.write_all(bytes)?;
            temp.flush()?;
            temp.sync_all()?;
            self.validate_current()?;
            let result = unsafe {
                libc::renameat(
                    self.parent.as_raw_fd(),
                    temp_name.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.basename.as_ptr(),
                )
            };
            if result < 0 {
                return Err(std::io::Error::last_os_error()).context("原子替换失败");
            }
            self.parent.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            unsafe {
                libc::unlinkat(self.parent.as_raw_fd(), temp_name.as_ptr(), 0);
            }
        }
        result
    }

    fn validate_current(&self) -> Result<()> {
        let path_parent = self.path.parent().context("文件路径没有父目录")?;
        let current_parent = open_directory_tree(path_parent, false)?;
        if !self.parent_identity.matches(Identity::of(&current_parent)?) {
            bail!("父目录身份已变化，拒绝覆盖: {}", self.path.display());
        }
        let current = open_regular_at(self.parent.as_raw_fd(), &self.basename)?;
        match (&self.version, current) {
            (None, None) => Ok(()),
            (Some(expected), Some(file)) => {
                if !expected.identity.matches(Identity::of(&file)?) || file.metadata()?.nlink() > 1
                {
                    bail!("目标身份或链接数已变化，拒绝覆盖: {}", self.path.display());
                }
                let actual = digest_file(&file)?;
                if actual != expected.sha256 {
                    bail!("文件内容已变化，拒绝覆盖: {}", self.path.display());
                }
                Ok(())
            }
            _ => bail!("目标存在状态已变化，拒绝覆盖: {}", self.path.display()),
        }
    }
}

fn version_of(file: &File) -> Result<Version> {
    Ok(Version {
        identity: Identity::of(file)?,
        sha256: digest_file(file)?,
    })
}

fn digest_file(file: &File) -> Result<[u8; 32]> {
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest.finalize().into())
}

fn c_name(name: &OsStr) -> Result<CString> {
    Ok(CString::new(name.as_bytes())?)
}

fn open_at(parent: RawFd, name: &CString, flags: i32, mode: libc::mode_t) -> Result<File> {
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode as libc::c_uint,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_regular_at(parent: RawFd, name: &CString) -> Result<Option<File>> {
    let file = match open_at(parent, name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
        Ok(file) => file,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error).context("安全打开目标失败"),
    };
    if !file.metadata()?.is_file() {
        bail!("目标不是普通文件");
    }
    Ok(Some(file))
}

fn open_directory_tree(path: &Path, create: bool) -> Result<File> {
    if !path.is_absolute() {
        bail!("目录路径必须是绝对路径");
    }
    let mut current = File::open("/")?;
    for component in path.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        let name = c_name(part)?;
        let flags = libc::O_RDONLY | libc::O_DIRECTORY;
        let next = match open_at(current.as_raw_fd(), &name, flags, 0) {
            Ok(dir) => dir,
            Err(error)
                if create
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                let created = unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o755) };
                if created < 0
                    && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
                {
                    return Err(std::io::Error::last_os_error()).context("创建父目录失败");
                }
                open_at(current.as_raw_fd(), &name, flags, 0)?
            }
            Err(error) => return Err(error).context("安全打开父目录失败"),
        };
        current = next;
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn root() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent-cap-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::canonicalize(root).unwrap()
    }

    #[test]
    fn rejects_symlink_and_hardlink_write() {
        let dir = root();
        let target = dir.join("target");
        std::fs::write(&target, "safe").unwrap();
        let link = dir.join("link");
        symlink(&target, &link).unwrap();
        assert!(AuthorizedPath::open(link, FileAccessIntent::Write).is_err());
        let hard = dir.join("hard");
        std::fs::hard_link(&target, &hard).unwrap();
        assert!(AuthorizedPath::open(hard, FileAccessIntent::Write).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn detects_content_and_path_replacement_drift() {
        let dir = root();
        let path = dir.join("file");
        std::fs::write(&path, "old").unwrap();
        let capability = AuthorizedPath::open(path.clone(), FileAccessIntent::Edit).unwrap();
        std::fs::write(&path, "changed").unwrap();
        assert!(capability.atomic_write(b"new").is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "changed");
        let capability = AuthorizedPath::open(path.clone(), FileAccessIntent::Edit).unwrap();
        std::fs::remove_file(&path).unwrap();
        symlink(dir.join("other"), &path).unwrap();
        assert!(capability.atomic_write(b"new").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn creates_missing_parents_and_refuses_parent_replacement() {
        let dir = root();
        let path = dir.join("a/b/new.txt");
        let capability = AuthorizedPath::open(path.clone(), FileAccessIntent::Write).unwrap();
        capability.atomic_write(b"hello").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        let second =
            AuthorizedPath::open(dir.join("a/b/next.txt"), FileAccessIntent::Write).unwrap();
        std::fs::rename(dir.join("a/b"), dir.join("moved")).unwrap();
        std::fs::create_dir(dir.join("a/b")).unwrap();
        assert!(second.atomic_write(b"no").is_err());
        assert!(!dir.join("a/b/next.txt").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn atomic_edit_preserves_existing_mode() {
        let dir = root();
        let path = dir.join("script.sh");
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        AuthorizedPath::open(path.clone(), FileAccessIntent::Edit)
            .unwrap()
            .atomic_write(b"new")
            .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o755
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
