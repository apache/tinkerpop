/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// slowReader simulates a network stream that delivers data in chunks with delays.
// This mimics how Go's HTTP client receives chunked transfer-encoded responses
// where chunk boundaries don't align with GraphBinary object boundaries.
type slowReader struct {
	chunks [][]byte
	delay  time.Duration
	index  int
	offset int
	mu     sync.Mutex
}

func newSlowReader(chunks [][]byte, delay time.Duration) *slowReader {
	return &slowReader{chunks: chunks, delay: delay}
}

func (r *slowReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}

	// Simulate network delay between chunks
	if r.offset == 0 && r.index > 0 {
		r.mu.Unlock()
		time.Sleep(r.delay)
		r.mu.Lock()
	}

	chunk := r.chunks[r.index]
	remaining := chunk[r.offset:]
	n = copy(p, remaining)
	r.offset += n

	if r.offset >= len(chunk) {
		r.index++
		r.offset = 0
	}

	return n, nil
}

func TestStreamingDeserializer(t *testing.T) {
	t.Run("readInt32", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x2A} // 42
		d := NewStreamingDeserializer(bytes.NewReader(data))
		val, err := d.readInt32()
		assert.Nil(t, err)
		assert.Equal(t, int32(42), val)
	})

	t.Run("readInt64", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64} // 100
		d := NewStreamingDeserializer(bytes.NewReader(data))
		val, err := d.readInt64()
		assert.Nil(t, err)
		assert.Equal(t, int64(100), val)
	})

	t.Run("readString", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}
		d := NewStreamingDeserializer(bytes.NewReader(data))
		val, err := d.readString()
		assert.Nil(t, err)
		assert.Equal(t, "hello", val)
	})

	t.Run("readString empty", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x00}
		d := NewStreamingDeserializer(bytes.NewReader(data))
		val, err := d.readString()
		assert.Nil(t, err)
		assert.Equal(t, "", val)
	})

	t.Run("error on incomplete data", func(t *testing.T) {
		data := []byte{0x00, 0x00} // incomplete int32
		d := NewStreamingDeserializer(bytes.NewReader(data))
		_, err := d.readInt32()
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("sticky error", func(t *testing.T) {
		data := []byte{0x00} // too short
		d := NewStreamingDeserializer(bytes.NewReader(data))

		_, err1 := d.readInt32()
		assert.Error(t, err1)

		// Subsequent reads should also fail
		_, err2 := d.readInt32()
		assert.Error(t, err2)
	})
}

func TestStreamingChannelDelivery(t *testing.T) {
	t.Run("results arrive incrementally via channel", func(t *testing.T) {
		rs := newChannelResultSet()

		// Simulate streaming - send results with delays
		go func() {
			for i := 0; i < 5; i++ {
				time.Sleep(10 * time.Millisecond)
				rs.Channel() <- &Result{i}
			}
			rs.Close()
		}()

		var times []time.Duration
		start := time.Now()

		for {
			_, ok, _ := rs.One()
			if !ok {
				break
			}
			times = append(times, time.Since(start))
		}

		assert.Equal(t, 5, len(times))

		// Results should arrive ~10ms apart, not all at once
		for i := 1; i < len(times); i++ {
			gap := times[i] - times[i-1]
			assert.GreaterOrEqual(t, gap, 5*time.Millisecond,
				"Results %d and %d arrived too close together: %v", i-1, i, gap)
		}
	})
}

// TestStreamingBlocksOnPartialData verifies that the deserializer correctly blocks
// when it receives partial data, waiting for the rest of the object to arrive.
// This simulates the real-world scenario where Go's HTTP client receives chunks
// that don't align with server-sent GraphBinary object boundaries.
func TestStreamingBlocksOnPartialData(t *testing.T) {
	t.Run("blocks until complete int32 is available", func(t *testing.T) {
		// Split a 4-byte int32 across two chunks
		chunk1 := []byte{0x00, 0x00} // First 2 bytes
		chunk2 := []byte{0x00, 0x2A} // Last 2 bytes (total = 42)

		reader := newSlowReader([][]byte{chunk1, chunk2}, 20*time.Millisecond)
		d := NewStreamingDeserializer(reader)

		start := time.Now()
		val, err := d.readInt32()
		elapsed := time.Since(start)

		assert.Nil(t, err)
		assert.Equal(t, int32(42), val)
		// Should have blocked waiting for second chunk
		assert.GreaterOrEqual(t, elapsed, 15*time.Millisecond,
			"Should have blocked waiting for remaining bytes")
	})

	t.Run("blocks until complete string is available", func(t *testing.T) {
		// String "hello" split across chunks:
		// Chunk 1: length (4 bytes) + partial content
		// Chunk 2: remaining content
		chunk1 := []byte{0x00, 0x00, 0x00, 0x05, 'h', 'e'} // length=5, "he"
		chunk2 := []byte{'l', 'l', 'o'}                    // "llo"

		reader := newSlowReader([][]byte{chunk1, chunk2}, 20*time.Millisecond)
		d := NewStreamingDeserializer(reader)

		start := time.Now()
		val, err := d.readString()
		elapsed := time.Since(start)

		assert.Nil(t, err)
		assert.Equal(t, "hello", val)
		assert.GreaterOrEqual(t, elapsed, 15*time.Millisecond,
			"Should have blocked waiting for remaining string bytes")
	})
}

// TestStreamingMultipleObjects verifies that multiple GraphBinary objects
// can be read from a stream, with each object returned as soon as it's complete.
func TestStreamingMultipleObjects(t *testing.T) {
	t.Run("reads multiple objects as they arrive", func(t *testing.T) {
		// Build a stream with 3 fully-qualified int32 values
		// Each int32: type(1) + flag(1) + value(4) = 6 bytes
		obj1 := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x01} // int32 = 1
		obj2 := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x02} // int32 = 2
		obj3 := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x03} // int32 = 3

		// Deliver each object as a separate chunk with delay
		reader := newSlowReader([][]byte{obj1, obj2, obj3}, 15*time.Millisecond)
		d := NewStreamingDeserializer(reader)

		var results []int32
		var times []time.Duration
		start := time.Now()

		for i := 0; i < 3; i++ {
			val, err := d.ReadFullyQualified()
			assert.Nil(t, err)
			results = append(results, val.(int32))
			times = append(times, time.Since(start))
		}

		assert.Equal(t, []int32{1, 2, 3}, results)

		// Objects should arrive with delays between them
		for i := 1; i < len(times); i++ {
			gap := times[i] - times[i-1]
			assert.GreaterOrEqual(t, gap, 10*time.Millisecond,
				"Object %d should have arrived after a delay", i)
		}
	})

	t.Run("handles object split across chunk boundary", func(t *testing.T) {
		// First chunk: complete object + partial second object
		// Second chunk: rest of second object + complete third object
		chunk1 := []byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x01, // int32 = 1 (complete)
			0x01, 0x00, 0x00, // partial int32 (type + flag + 2 bytes of value)
		}
		chunk2 := []byte{
			0x00, 0x00, 0x02, // rest of int32 = 2
			0x01, 0x00, 0x00, 0x00, 0x00, 0x03, // int32 = 3 (complete)
		}

		reader := newSlowReader([][]byte{chunk1, chunk2}, 20*time.Millisecond)
		d := NewStreamingDeserializer(reader)

		// First object should return immediately
		val1, err := d.ReadFullyQualified()
		assert.Nil(t, err)
		assert.Equal(t, int32(1), val1)

		// Second object should block waiting for chunk2
		start := time.Now()
		val2, err := d.ReadFullyQualified()
		elapsed := time.Since(start)
		assert.Nil(t, err)
		assert.Equal(t, int32(2), val2)
		assert.GreaterOrEqual(t, elapsed, 15*time.Millisecond,
			"Should have blocked waiting for rest of object")

		// Third object should return immediately (already in buffer)
		val3, err := d.ReadFullyQualified()
		assert.Nil(t, err)
		assert.Equal(t, int32(3), val3)
	})
}

// TestStreamingWithEndOfStreamMarker verifies that the deserializer correctly
// handles the EndOfStream marker and subsequent status reading.
func TestStreamingWithEndOfStreamMarker(t *testing.T) {
	t.Run("reads EndOfStream marker and status", func(t *testing.T) {
		// Build a complete response:
		// - Header (2 bytes): version + flags
		// - One int32 result
		// - EndOfStream marker
		// - Status: code(4) + message(nullable) + exception(nullable)
		data := []byte{
			0x81, 0x00, // Header: version byte + no bulking
			0x01, 0x00, 0x00, 0x00, 0x00, 0x2A, // int32 = 42
			0xfd, 0x00, 0x00, // Marker type + flag + value=0 (EndOfStream)
			0x00, 0x00, 0x00, 0xC8, // Status code = 200
			0x01, // Message is null
			0x01, // Exception is null
		}

		d := NewStreamingDeserializer(bytes.NewReader(data))

		// Read header
		err := d.ReadHeader()
		assert.Nil(t, err)

		// Read the result
		val, err := d.ReadFullyQualified()
		assert.Nil(t, err)
		assert.Equal(t, int32(42), val)

		// Read EndOfStream marker
		marker, err := d.ReadFullyQualified()
		assert.Nil(t, err)
		assert.Equal(t, EndOfStream(), marker)

		// Read status
		code, msg, exc, err := d.ReadStatus()
		assert.Nil(t, err)
		assert.Equal(t, uint32(200), code)
		assert.Equal(t, "", msg)
		assert.Equal(t, "", exc)
	})

	t.Run("reads status with message", func(t *testing.T) {
		// Status with a message
		data := []byte{
			0xfd, 0x00, 0x00, // EndOfStream marker
			0x00, 0x00, 0x01, 0x90, // Status code = 400
			0x00,                                            // Message is not null
			0x00, 0x00, 0x00, 0x05, 'e', 'r', 'r', 'o', 'r', // Message = "error"
			0x01, // Exception is null
		}

		d := NewStreamingDeserializer(bytes.NewReader(data))

		marker, err := d.ReadFullyQualified()
		assert.Nil(t, err)
		assert.Equal(t, EndOfStream(), marker)

		code, msg, exc, err := d.ReadStatus()
		assert.Nil(t, err)
		assert.Equal(t, uint32(400), code)
		assert.Equal(t, "error", msg)
		assert.Equal(t, "", exc)
	})
}

// TestStreamingComplexTypes verifies streaming deserialization of complex types
// like vertices, edges, and paths.
func TestStreamingComplexTypes(t *testing.T) {
	t.Run("reads vertex from stream", func(t *testing.T) {
		// Vertex: id(int32) + labels(list of string) + properties(nullable)
		data := []byte{
			0x11, 0x00, // Vertex type + flag
			// ID: int32 = 1
			0x01, 0x00, 0x00, 0x00, 0x00, 0x01,
			// Labels: list with one string "person"
			0x00, 0x00, 0x00, 0x01, // list length = 1
			0x03, 0x00, // string type + flag
			0x00, 0x00, 0x00, 0x06, 'p', 'e', 'r', 's', 'o', 'n',
			// Properties: null
			0xfe, 0x01,
		}

		d := NewStreamingDeserializer(bytes.NewReader(data))
		val, err := d.ReadFullyQualified()
		assert.Nil(t, err)

		v, ok := val.(*Vertex)
		assert.True(t, ok)
		assert.Equal(t, int32(1), v.Id)
		assert.Equal(t, "person", v.Label)
	})

	t.Run("reads list of integers from chunked stream", func(t *testing.T) {
		// List type split across chunks
		chunk1 := []byte{
			0x09, 0x00, // List type + flag
			0x00, 0x00, 0x00, 0x03, // length = 3
			0x01, 0x00, 0x00, 0x00, 0x00, 0x0A, // int32 = 10
		}
		chunk2 := []byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x14, // int32 = 20
			0x01, 0x00, 0x00, 0x00, 0x00, 0x1E, // int32 = 30
		}

		reader := newSlowReader([][]byte{chunk1, chunk2}, 15*time.Millisecond)
		d := NewStreamingDeserializer(reader)

		start := time.Now()
		val, err := d.ReadFullyQualified()
		elapsed := time.Since(start)

		assert.Nil(t, err)
		list, ok := val.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 3, len(list))
		assert.Equal(t, int32(10), list[0])
		assert.Equal(t, int32(20), list[1])
		assert.Equal(t, int32(30), list[2])

		// Should have blocked for second chunk
		assert.GreaterOrEqual(t, elapsed, 10*time.Millisecond)
	})
}
