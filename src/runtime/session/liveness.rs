use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::oneshot;

use crate::runtime::SdkError;

#[cfg(unix)]
const POLL_INTERVAL_MILLISECONDS: libc::c_int = 50;

pub(super) struct ParentLiveness {
    closed: oneshot::Receiver<std::io::Result<()>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ParentLiveness {
    pub(super) fn start() -> Result<Self, SdkError> {
        let (closed, receiver) = oneshot::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("oll-parent-liveness".to_owned())
            .spawn(move || watch_stdin(thread_stop, closed))
            .map_err(|error| SdkError::runtime("start parent-liveness watcher", error))?;
        Ok(Self {
            closed: receiver,
            stop,
            thread: Some(thread),
        })
    }

    pub(super) async fn wait(&mut self) -> Result<(), SdkError> {
        (&mut self.closed)
            .await
            .map_err(|error| SdkError::runtime("wait for parent-liveness watcher", error))?
            .map_err(|error| SdkError::runtime("watch parent-liveness pipe", error))
    }
}

impl Drop for ParentLiveness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            #[cfg(unix)]
            let _ = thread.join();
            #[cfg(not(unix))]
            // Portable stdio offers no way to interrupt a blocked stdin read.
            // The plugin process exits when `Plugin::run` returns; detaching
            // avoids deadlocking that exit on platforms without `poll`.
            drop(thread);
        }
    }
}

#[cfg(unix)]
fn watch_stdin(stop: Arc<AtomicBool>, closed: oneshot::Sender<std::io::Result<()>>) {
    let result = watch_descriptor(libc::STDIN_FILENO, &stop);
    if !stop.load(Ordering::Acquire) {
        let _ = closed.send(result);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd as _, OwnedFd};

    use super::*;

    fn pipe() -> (OwnedFd, OwnedFd) {
        let mut descriptors = [-1; 2];
        // SAFETY: `descriptors` has room for the two file descriptors written
        // by `pipe`. Successful descriptors are immediately owned by OwnedFd.
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        // SAFETY: a successful `pipe` call returned two new owned descriptors.
        unsafe {
            (
                OwnedFd::from_raw_fd(descriptors[0]),
                OwnedFd::from_raw_fd(descriptors[1]),
            )
        }
    }

    #[test]
    fn descriptor_watcher_returns_on_eof() {
        let (reader, writer) = pipe();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::spawn(move || {
            watch_descriptor(reader.as_raw_fd(), &thread_stop).unwrap();
        });
        drop(writer);
        thread.join().unwrap();
    }

    #[test]
    fn descriptor_watcher_can_be_stopped_without_eof() {
        let (reader, _writer) = pipe();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::spawn(move || {
            watch_descriptor(reader.as_raw_fd(), &thread_stop).unwrap();
        });
        stop.store(true, Ordering::Release);
        thread.join().unwrap();
    }

    #[test]
    fn descriptor_watcher_reports_io_failures() {
        let (reader, _writer) = pipe();
        let descriptor = reader.as_raw_fd();
        drop(reader);
        assert!(watch_descriptor(descriptor, &AtomicBool::new(false)).is_err());
    }
}

#[cfg(unix)]
fn watch_descriptor(descriptor: libc::c_int, stop: &AtomicBool) -> std::io::Result<()> {
    let mut event = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
        revents: 0,
    };
    let mut byte = 0_u8;
    while !stop.load(Ordering::Acquire) {
        // SAFETY: `event` points to one initialized pollfd for the duration of
        // the call. The descriptor is borrowed from the process and never
        // closed or mutated by this watcher.
        let ready = unsafe { libc::poll(&mut event, 1, POLL_INTERVAL_MILLISECONDS) };
        if ready == 0 {
            continue;
        }
        if ready < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        // SAFETY: `byte` is valid for a one-byte write and `poll` reported the
        // borrowed descriptor as readable, hung up, or failed.
        let read = unsafe {
            libc::read(
                descriptor,
                std::ptr::from_mut(&mut byte).cast::<libc::c_void>(),
                1,
            )
        };
        if read == 0 {
            return Ok(());
        }
        if read < 0 {
            let error = std::io::Error::last_os_error();
            if matches!(
                error.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn watch_stdin(stop: Arc<AtomicBool>, closed: oneshot::Sender<std::io::Result<()>>) {
    use std::io::Read as _;

    let mut input = std::io::stdin().lock();
    let mut buffer = [0_u8; 1];
    let result = loop {
        if stop.load(Ordering::Acquire) {
            break Ok(());
        }
        match input.read(&mut buffer) {
            Ok(0) => break Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => break Err(error),
        }
    };
    if !stop.load(Ordering::Acquire) {
        let _ = closed.send(result);
    }
}
