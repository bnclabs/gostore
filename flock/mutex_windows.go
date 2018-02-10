package filemutex

import "sync"
import "syscall"
import "unsafe"

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

func lockFileEx(
	h syscall.Handle, flags, reserved, locklow, lockhigh uint32,
	ol *syscall.Overlapped) (err error) {

	r1, _, e1 := syscall.Syscall6(
		procLockFileEx.Addr(), 6, uintptr(h), uintptr(flags),
		uintptr(reserved),
		uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))

	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func unlockFileEx(
	h syscall.Handle, reserved, locklow, lockhigh uint32,
	ol *syscall.Overlapped) (err error) {

	r1, _, e1 := syscall.Syscall6(procUnlockFileEx.Addr(), 5, uintptr(h),
		uintptr(reserved),
		uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)), 0)

	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

// RWMutex is equivalent to sync.RWMutex, but synchronizes across processes.
type RWMutex struct {
	mu sync.RWMutex
	fd syscall.Handle
}

// New create a new instance of multi-process rwmutex.
func New(filename string) (*RWMutex, error) {
	fd, err := syscall.CreateFile(
		&(syscall.StringToUTF16(filename)[0]),
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)

	if err != nil {
		return nil, err
	}
	return &RWMutex{fd: fd}, nil
}

// Lock locks rw. If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (rw *RWMutex) Lock() {
	rw.mu.Lock()
	var ol syscall.Overlapped
	err := lockFileEx(rw.fd, 2 /*lockfileExclusiveLock*/, 0, 1, 0, &ol)
	if err != nil {
		panic(err)
	}
}

// Unlock unlocks rw. It is a run-time error if rw is not locked on entry to
// Unlock.
func (rw *RWMutex) Unlock() {
	var ol syscall.Overlapped
	if err := unlockFileEx(rw.fd, 0, 1, 0, &ol); err != nil {
		panic(err)
	}
	rw.mu.Unlock()
}

// RLock locks rw for reading. It should not be used for recursive read
// locking; a blocked Lock call excludes new readers from acquiring the lock.
func (rw *RWMutex) RLock() {
	rw.mu.RLock()
	var ol syscall.Overlapped
	if err := lockFileEx(rw.fd, 0, 0, 1, 0, &ol); err != nil {
		panic(err)
	}
}

// RUnlock undo a single RLock call; it does not affect other
// simultaneous readers.
func (rw *RWMutex) RUnlock() {
	var ol syscall.Overlapped
	if err := unlockFileEx(rw.fd, 0, 1, 0, &ol); err != nil {
		panic(err)
	}
	rw.mu.RUnlock()
}
