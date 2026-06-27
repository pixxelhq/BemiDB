package main

import (
	"errors"
	"io"
	"sync"
)

type CappedBuffer struct {
	config       *Config
	maxSizeBytes int

	buffer          []byte
	mutex           sync.Mutex
	conditionalSync *sync.Cond

	closeOnceSync sync.Once
	closed        bool
	closeErr      error
}

func NewCappedBuffer(maxSizeBytes int, config *Config) *CappedBuffer {
	sizedBuffer := &CappedBuffer{
		config:       config,
		buffer:       make([]byte, 0, maxSizeBytes),
		maxSizeBytes: maxSizeBytes,
	}
	sizedBuffer.conditionalSync = sync.NewCond(&sizedBuffer.mutex)
	return sizedBuffer
}

// Implements io.Writer
func (buf *CappedBuffer) Write(payload []byte) (writtenBytes int, err error) {
	if len(payload) == 0 {
		return 0, nil
	}

	buf.mutex.Lock()
	defer buf.mutex.Unlock()

	if buf.closed {
		return 0, errors.New("buffer is closed")
	}

	for len(buf.buffer)+len(payload) > buf.maxSizeBytes && !buf.closed {
		LogTrace(buf.config, ">> Waiting for more space in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the reader
	}

	// Check again if buffer was closed while waiting
	if buf.closed {
		return 0, errors.New("buffer is closed")
	}

	writtenBytes = len(payload)
	buf.buffer = append(buf.buffer, payload...)
	LogTrace(buf.config, ">> Writing", writtenBytes, "bytes to capped buffer...")

	buf.conditionalSync.Broadcast() // Notify the reader that new data is available

	return writtenBytes, nil
}

// Implements io.Reader
func (buf *CappedBuffer) Read(payload []byte) (readBytes int, err error) {
	if len(payload) == 0 {
		return 0, nil
	}

	buf.mutex.Lock()
	defer buf.mutex.Unlock()

	for len(buf.buffer) == 0 && !buf.closed {
		LogTrace(buf.config, "<< Waiting for more data in capped buffer...")
		buf.conditionalSync.Wait() // Wait for the writer
	}

	if len(buf.buffer) == 0 && buf.closed {
		// Once drained, surface the reason the buffer was closed. A clean Close()
		// means we genuinely reached the end of the data (io.EOF). A CloseWithError()
		// (e.g. the COPY was aborted mid-stream by a hot-standby recovery conflict)
		// must NOT look like a clean end, otherwise callers commit partial data as complete.
		if buf.closeErr != nil {
			return 0, buf.closeErr
		}
		return 0, io.EOF
	}

	maxReadBytes := len(payload)
	readBytes = copy(payload, buf.buffer)
	buf.buffer = buf.buffer[readBytes:]
	LogTrace(buf.config, "<< Reading "+IntToString(readBytes)+"/"+IntToString(maxReadBytes)+" bytes from capped buffer...")

	buf.conditionalSync.Broadcast() // Notify the writer that space is now available

	return readBytes, nil
}

func (buf *CappedBuffer) Close() error {
	return buf.CloseWithError(nil)
}

// CloseWithError closes the buffer and records why. Readers drain any remaining
// buffered data first, then receive err (or io.EOF when err is nil). The error is
// set before waking readers, so a reader that observes the buffer as closed always
// sees the final close reason — no race with the goroutine producing the data.
func (buf *CappedBuffer) CloseWithError(err error) error {
	buf.closeOnceSync.Do(func() {
		buf.mutex.Lock()

		LogTrace(buf.config, "== Closing capped buffer...")
		buf.closeErr = err
		buf.closed = true

		buf.conditionalSync.Broadcast() // Wake up any waiting writers/readers

		buf.mutex.Unlock()
	})
	return nil
}

// CloseError returns the error the buffer was closed with, or nil for a clean close
// (or while still open).
func (buf *CappedBuffer) CloseError() error {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	return buf.closeErr
}
