Writeup on how to use initialize logging using SetLogger(). There are one ore
two ways of doing it.

- importing github.com/prataprc/storage.go/log will automatically initialize
  logging to console with default level.
- initialize logging under application's init() function.
- initialize logging under application's main().
- important to initialize logging before using storage.go package.
