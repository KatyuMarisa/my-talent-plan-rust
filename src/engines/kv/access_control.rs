use std::sync::{
    atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed, SeqCst},
    }
};

/// A read/write access controller for outer struct. The only member is an `AtomicUsize`, we use bit
/// operation to controll access. The usage of bits is below:
/// 
/// |    UNUSED    |_BIT_PAUSE_ALL|_BIT_PAUSE_WRITE| Write Pincount | Read Pincount |
/// | 63........22 |      21      |       20       | 19..........10 | 9...........0 |
/// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
/// +                           A t o m i c U s i z e                               *
/// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
/// 
pub struct AccessController {
    _pin_count: AtomicUsize,
}

impl Default for AccessController {
    fn default() -> Self {
        Self { _pin_count: AtomicUsize::new(0) }
    }
}

impl AccessController {
    const _BITSHFT_READ: usize = 10;
    const _BITSHFT_WRITE: usize = 10;

    const _BIT_PINREAD_END: usize = 1 << Self::_BITSHFT_READ;
    const _BIT_PINWRITE_END: usize = 1 << (Self::_BITSHFT_READ + Self::_BITSHFT_WRITE);
    const _BIT_PAUSE_WRITE: usize = 1 << (Self::_BITSHFT_READ + Self::_BITSHFT_WRITE + 1);
    const _BIT_PAUSE_ALL: usize = 1 << (Self::_BITSHFT_READ + Self::_BITSHFT_WRITE + 2);

    /// Outer requests for a read operation.
    /// During the lifetime of `CtrlGuard` returned from this function, all Outer's read operation is valid. Outer's
    /// read operation without holding `CtrlGuard` returned from this function is undefined behaviour.
    pub fn guard_read(&self) -> Option<CtrlGuard> {
        let pin = self._pin_count.load(Relaxed);
        if Self::_read_allow(pin) &&
            self._pin_count.compare_exchange(pin, pin + 1, Acquire, Relaxed).is_ok() {
            let destructor = Box::new(|ct: &AccessController| {
                ct._pin_count.fetch_sub(1, SeqCst);
            });
            
            return Some(CtrlGuard { ctrl: self, destructor })
        }
        None
    }

    /// Outer requests for a write operation.
    /// During the lifetime of `CtrlGuard` returned from this function, all Outer's read requests is valid. Outer's
    /// write operation without holding `CtrlGuard` returned from this function is undefined behaviour.
    pub fn guard_write(&self) -> Option<CtrlGuard> {
        let pin = self._pin_count.load(Relaxed);
        if Self::_write_allow(pin) &&
            self._pin_count.compare_exchange(pin, pin + (1<<10), Acquire, Relaxed).is_ok() {
            let destructor = Box::new(|ct: &AccessController| {
                ct._pin_count.fetch_sub(1<<10, Acquire);
            });

            return Some(CtrlGuard { ctrl: self, destructor })
        }
        None
    }

    /// Pause all outer's write requests.
    /// During the lifetime of `CtrlGuard` returned from this function, all `Controller::guard_write`
    /// is guaranteed to return None, and all remaining write requests is drained. A write operation
    pub fn pause_write(&self) -> CtrlGuard {
        // reject all incoming write requests
        loop {
            let pin = self._pin_count.load(Relaxed);
            if self._pin_count.compare_exchange(pin, pin | Self::_BIT_PAUSE_WRITE, Acquire, Relaxed).is_ok() {
                break;
            }
        }
        // all incoming write requests is rejected, busy wait for all remaining write requests finish...
        loop {
            let pin = self._pin_count.load(Relaxed);
            if Self::_write_requests_drained(pin) {
                break;
            }
        }

        let destructor = Box::new(|ct: &AccessController| {
            ct._pin_count.fetch_sub(Self::_BIT_PAUSE_WRITE, Acquire);
        });

        CtrlGuard { ctrl: self, destructor }
    }

    /// Pause all outer's read requests and write requests.
    /// With the lifetimes of `CtrlGuard` returned from this function, all `Controller::guard_read`
    /// and `Controller::guard_write` is guaranteed to return None, and all remaining read/write
    /// requests is drained.
    #[allow(dead_code)]
    pub fn pause_all(&self) -> CtrlGuard {
        loop {
            let pin = self._pin_count.load(Relaxed);
            if self._pin_count.compare_exchange(pin, pin | Self::_BIT_PAUSE_ALL, Acquire, Relaxed).is_ok() {
                break;
            }
        }

        loop {
            let pin = self._pin_count.load(Relaxed);
            if Self::_all_requests_drained(pin) {
                break;
            }
        }

        let destructor = Box::new(|ct: &AccessController| {
            ct._pin_count.compare_exchange(Self::_BIT_PAUSE_ALL, 0, Acquire, Relaxed).unwrap();
        });

        CtrlGuard { ctrl: self, destructor }
    }

    pub fn check_write_paused(&self) -> bool {
        let pin = self._pin_count.load(Relaxed);
        Self::_write_requests_drained(pin)
    }

    #[allow(dead_code)]
    pub fn check_all_paused(&self) -> bool {
        let pin = self._pin_count.load(Relaxed);
        Self::_all_requests_drained(pin)
    }

    pub fn check_no_pending_requests(&self) -> bool {
        let pin = self._pin_count.load(Relaxed);
        pin == 0 || pin == Self::_BIT_PAUSE_ALL
    }

    #[inline(always)]
    fn _read_allow(pin: usize) -> bool {
        (Self::_BIT_PAUSE_ALL..).contains(&pin)
    }

    #[inline(always)]
    fn _write_allow(pin: usize) -> bool {
        (Self::_BIT_PAUSE_WRITE..).contains(&pin)
    }

    #[inline(always)]
    fn _write_requests_drained(pin: usize) -> bool {
        for i in Self::_BITSHFT_READ..Self::_BITSHFT_WRITE + Self::_BITSHFT_WRITE {
            if ((1 << i) & pin) != 0 {
                return false
            } 
        }
        true
    }

    #[inline(always)]
    fn _all_requests_drained(pin: usize) -> bool {
        pin == Self::_BIT_PAUSE_ALL
    }
}

pub struct CtrlGuard<'a>
{
    ctrl: &'a AccessController,
    // TODO: Use FnOnce instead of Fn
    destructor: Box<dyn Fn(&AccessController)>,
}

impl<'a> Drop for CtrlGuard<'a>
{
    fn drop(&mut self) {
        (self.destructor)(self.ctrl);
    }
}
