package flock

import "os"
import "fmt"
import "testing"
import "io/ioutil"
import "path/filepath"

import "github.com/stretchr/testify/assert"
import "github.com/stretchr/testify/require"

func TestLockUnlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "x")
	m, err := New(path)
	require.NoError(t, err)

	m.Lock()
	m.Unlock()
}

func TestRLockUnlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "x")
	m, err := New(path)
	fmt.Println(path)
	require.NoError(t, err)

	m.RLock()
	m.RUnlock()
}

func TestSimultaneousLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "x")
	m, err := New(path)
	require.NoError(t, err)

	m.Lock()

	state := "waiting"
	ch := make(chan struct{})
	go func() {
		m.Lock()
		state = "acquired"
		ch <- struct{}{}

		<-ch
		m.Unlock()
		state = "released"
		ch <- struct{}{}
	}()

	assert.Equal(t, "waiting", state)
	m.Unlock()

	<-ch
	assert.Equal(t, "acquired", state)
	ch <- struct{}{}

	<-ch
	assert.Equal(t, "released", state)
}

func TestSimultaneousRLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "x")
	m, err := New(path)
	require.NoError(t, err)

	m.RLock()

	state := "waiting"
	ch := make(chan struct{})
	go func() {
		m.RLock()
		state = "racquired"
		ch <- struct{}{}

		<-ch
		m.RUnlock()

		m.Lock()
		state = "acquired"
		ch <- struct{}{}

		<-ch
		m.Unlock()
		state = "released"
		ch <- struct{}{}
	}()

	<-ch
	assert.Equal(t, "racquired", state)
	m.RUnlock()
	ch <- struct{}{}

	<-ch
	assert.Equal(t, "acquired", state)
	ch <- struct{}{}

	<-ch
	assert.Equal(t, "released", state)
}
