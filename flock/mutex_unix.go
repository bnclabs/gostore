// +build darwin dragonfly freebsd linux netbsd openbsd

package flock

import "sync"
import "syscall"

// RWMutex is equivalent to sync.RWMutex, but synchronizes across processes.
type RWMutex struct {
	mu sync.RWMutex
	fd int
}

// New create a new instance of multi-process rwmutex.
func New(filename string) (*RWMutex, error) {
	fd, err := syscall.Open(filename, syscall.O_CREAT|syscall.O_RDONLY, 0750)
	if err != nil {
		return nil, err
	}
	return &RWMutex{fd: fd}, nil
}

// Lock locks m. If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (rw *RWMutex) Lock() {
	rw.mu.Lock()
	if err := syscall.Flock(rw.fd, syscall.LOCK_EX); err != nil {
		panic(err)
	}
}

// Unlock unlocks m. It is a run-time error if m is not locked on entry to
// Unlock.
func (rw *RWMutex) Unlock() {
	if err := syscall.Flock(rw.fd, syscall.LOCK_UN); err != nil {
		panic(err)
	}
	rw.mu.Unlock()
}

// RLock locks rw for reading.
//
// It should not be used for recursive read locking; a blocked Lock call
// excludes new readers from acquiring the lock.
func (rw *RWMutex) RLock() {
	rw.mu.RLock()
	if err := syscall.Flock(rw.fd, syscall.LOCK_SH); err != nil {
		panic(err)
	}
}

// RUnlock undoes a single RLock call; it does not affect other
// simultaneous readers. It is a run-time error if rw is not locked for
// reading on entry to RUnlock.
func (rw *RWMutex) RUnlock() {
	if err := syscall.Flock(rw.fd, syscall.LOCK_UN); err != nil {
		panic(err)
	}
	rw.mu.RUnlock()
}
