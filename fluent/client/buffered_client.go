package client

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/fluent-forward-go/fluent/protocol"
	"github.com/tinylib/msgp/msgp"
)

const (
	defaultBufferSize    = 1024 * 1024 // 1 MB
	defaultFlushInterval = 30 * time.Second
)

type BufferedClient struct {
	Client
	bufferSize    int
	flushInterval time.Duration
	writer        *msgp.Writer
	ticker        *time.Ticker
	mutex         sync.Mutex
	stop          chan struct{}
	done          chan struct{}
}

type BufferedClientConnectionOptions struct {
	ConnectionOptions
	BufferSize    int
	FlushInterval time.Duration
}

func NewBufferedClient(opts BufferedClientConnectionOptions) *BufferedClient {
	factory := opts.Factory
	if factory == nil {
		factory = &ConnFactory{
			Network: "tcp",
			Address: "localhost:24224",
		}
	}

	if opts.ConnectionTimeout == 0 {
		opts.ConnectionTimeout = DefaultConnectionTimeout
	}

	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufferSize
	}

	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaultFlushInterval
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)

		for {
			select {
			case <-ticker.C:
				bytes := atomic.LoadInt64(&totalBytesWritten)
				kilobytes := bytes / 1024
				totalNs := atomic.LoadInt64(&totalWriteTimeNs)
				totalMs := atomic.LoadInt64(&totalWriteTimeMs)
				count := atomic.LoadInt64(&writeCount)
				avgTimeNs := float64(totalNs) / float64(count) // Average time per write in nanoseconds
				avgTimeMs := float64(totalMs) / float64(count) // Average time per write in milliseconds

				fmt.Printf("**Buffered Client** Total Bytes Written: %d, Total Kilobytes Written: %d, Average Time per Write: %.2f ns, %.2f ms, Total writes: %d \n", bytes, kilobytes, avgTimeNs, avgTimeMs, count)

			}
		}
	}()

	bc := &BufferedClient{
		Client: Client{
			ConnectionFactory: factory,
			AuthInfo:          opts.AuthInfo,
			RequireAck:        opts.RequireAck,
			Timeout:           opts.ConnectionTimeout,
		},
		bufferSize:    opts.BufferSize,
		flushInterval: opts.FlushInterval,
		writer:        nil,
		ticker:        time.NewTicker(opts.FlushInterval),
		mutex:         sync.Mutex{},
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}

	return bc

}

func (bc *BufferedClient) flushLoop() {
	defer close(bc.done)

	for {
		select {
		case <-bc.ticker.C:
			_ = bc.Sync() // ignore error?
		case <-bc.stop:
			return
		}
	}
}

func (bc *BufferedClient) Sync() error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	return bc.writer.Flush()

}

func (bc *BufferedClient) Stop() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.ticker.Stop()
	close(bc.stop) // tell flushLoop to stop
	<-bc.done      // and wait until it has
}

// Connect initializes the Session and Connection objects by opening
// a client connect to the target configured in the ConnectionFactory
func (bc *BufferedClient) Connect() error {
	bc.sessionLock.Lock()
	defer bc.sessionLock.Unlock()

	if bc.session != nil {
		return errors.New("a session is already active")
	}

	return bc.connect()
}

// Disconnect terminates a client connection
func (bc *BufferedClient) Disconnect() error {
	return bc.Client.Disconnect()
}

func (bc *BufferedClient) Reconnect() error {
	return bc.Client.Reconnect()
}

func (bc *BufferedClient) SendRaw(m []byte) error {
	if bc.session == nil {
		return errors.New("no active session")
	}

	bc.sessionLock.RLock()
	defer bc.sessionLock.RUnlock()

	if !bc.session.TransportPhase {
		return errors.New("session handshake not completed")
	}

	// Start timing the write operation
	startTime := time.Now()

	// Write data to the buffer
	n, err := bc.writer.Write(m)
	// Update metrics
	durationNs := time.Since(startTime).Nanoseconds() // Duration in nanoseconds
	durationMs := durationNs / 1e6                    // Convert to milliseconds
	atomic.AddInt64(&totalBytesWritten, int64(n))
	atomic.AddInt64(&totalWriteTimeNs, durationNs)
	atomic.AddInt64(&totalWriteTimeMs, durationMs)
	atomic.AddInt64(&writeCount, 1)

	return err
}

func (bc *BufferedClient) Send(e protocol.ChunkEncoder) error {
	bc.sessionLock.RLock()
	defer bc.sessionLock.RUnlock()

	if bc.session == nil {
		return errors.New("no active session")
	}

	// Check if the session handshake is completed
	if !bc.session.TransportPhase {
		return errors.New("session handshake not completed")
	}

	// Handle RequireAck scenario
	var chunk string
	var err error
	if bc.Client.RequireAck {
		chunk, err = e.Chunk()
		if err != nil {
			return err
		}

		bc.Client.ackLock.Lock()
		defer bc.Client.ackLock.Unlock()
	}

	// Directly encode and handle errors without extra variable
	// if err = msgp.Encode(bc.session.Connection, e); err != nil {
	if err = e.EncodeMsg(bc.writer); err != nil {
		return err
	}

	// Only proceed to checkAck if RequireAck is true
	if bc.Client.RequireAck {
		return bc.checkAck(chunk)
	}

	return nil
}

func (bc *BufferedClient) SendCompressed(tag string, entries protocol.EntryList) error {
	return bc.Client.SendCompressed(tag, entries)
}

func (bc *BufferedClient) SendCompressedFromBytes(tag string, entries []byte) error {
	return bc.Client.SendCompressedFromBytes(tag, entries)
}

func (bc *BufferedClient) SendForward(tag string, entries protocol.EntryList) error {
	return bc.Client.SendForward(tag, entries)
}

func (bc *BufferedClient) SendMessage(tag string, record interface{}) error {
	return bc.Client.SendMessage(tag, record)
}

func (bc *BufferedClient) SendMessageExt(tag string, record interface{}) error {
	return bc.Client.SendMessageExt(tag, record)
}

func (bc *BufferedClient) SendPacked(tag string, entries protocol.EntryList) error {
	return bc.Client.SendPacked(tag, entries)
}

func (bc *BufferedClient) SendPackedFromBytes(tag string, entries []byte) error {
	return bc.Client.SendPackedFromBytes(tag, entries)
}

func (bc *BufferedClient) connect() error {
	err := bc.Client.connect()
	if err != nil {
		return err
	}

	// Create a buffered writer with a specified buffer size
	bc.writer = msgp.NewWriterSize(bc.Client.session.Connection, bc.bufferSize)

	go bc.flushLoop()

	return nil
}

func (bc *BufferedClient) disconnect() (err error) {
	return bc.Client.disconnect()
}
