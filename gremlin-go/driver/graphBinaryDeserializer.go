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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// GraphBinaryDeserializer reads GraphBinary data directly from an io.Reader,
// enabling streaming deserialization of server responses.
//
// Streaming Behavior:
// The deserializer is designed to work with HTTP chunked transfer encoding where
// the server streams results as they become available. Key behaviors:
//
//  1. Blocking on partial data: When reading an object, if the underlying reader
//     doesn't have enough bytes available, the deserializer blocks until the data
//     arrives. This is handled by io.ReadFull which waits for the exact number of
//     bytes needed based on GraphBinary's self-describing format (type codes and
//     length prefixes).
//
//  2. Chunk boundary independence: Go's HTTP client may receive data in chunks that
//     don't align with server-sent GraphBinary object boundaries. The deserializer
//     handles this transparently - it reads exactly the bytes needed for each object,
//     blocking if necessary, regardless of how the data was chunked by the network.
//
//  3. Immediate object delivery: Each complete object is returned as soon as it's
//     fully read, allowing the caller to process results incrementally rather than
//     waiting for the entire response.
//
// The bufio.Reader wrapper provides efficient buffering without affecting the
// streaming semantics - it simply reduces the number of underlying read syscalls.
type GraphBinaryDeserializer struct {
	r   *bufio.Reader
	buf [8]byte
	err error // sticky error
}

// GraphBinary flag for bulked list/set
const flagBulked = 0x02

// NewGraphBinaryDeserializer creates a new GraphBinaryDeserializer that reads from the given io.Reader.
// The reader is wrapped in a buffered reader for efficient reading.
func NewGraphBinaryDeserializer(r io.Reader) *GraphBinaryDeserializer {
	return &GraphBinaryDeserializer{r: bufio.NewReaderSize(r, 8192)}
}

func (d *GraphBinaryDeserializer) readByte() (byte, error) {
	if d.err != nil {
		return 0, d.err
	}
	b, err := d.r.ReadByte()
	if err != nil {
		d.err = err
		return 0, err
	}
	return b, nil
}

func (d *GraphBinaryDeserializer) readBytes(n int) ([]byte, error) {
	if d.err != nil {
		return nil, d.err
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(d.r, buf)
	if err != nil {
		d.err = err
		return nil, err
	}
	return buf, nil
}

func (d *GraphBinaryDeserializer) readInt32() (int32, error) {
	if d.err != nil {
		return 0, d.err
	}
	_, err := io.ReadFull(d.r, d.buf[:4])
	if err != nil {
		d.err = err
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(d.buf[:4])), nil
}

func (d *GraphBinaryDeserializer) readUint32() (uint32, error) {
	if d.err != nil {
		return 0, d.err
	}
	_, err := io.ReadFull(d.r, d.buf[:4])
	if err != nil {
		d.err = err
		return 0, err
	}
	return binary.BigEndian.Uint32(d.buf[:4]), nil
}

func (d *GraphBinaryDeserializer) readInt64() (int64, error) {
	if d.err != nil {
		return 0, d.err
	}
	_, err := io.ReadFull(d.r, d.buf[:8])
	if err != nil {
		d.err = err
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(d.buf[:8])), nil
}

// ReadHeader reads and validates the GraphBinary response header.
// This must be called before reading any objects from the stream.
func (d *GraphBinaryDeserializer) ReadHeader() error {
	if _, err := d.readByte(); err != nil {
		return err
	}
	_, err := d.readByte()
	return err
}

// ReadFullyQualified reads the next fully-qualified GraphBinary value from the stream.
// Returns the deserialized object, or an error if reading fails.
// When the end of the result stream is reached, this returns a Marker equal to EndOfStream().
func (d *GraphBinaryDeserializer) ReadFullyQualified() (interface{}, error) {
	dtByte, err := d.readByte()
	if err != nil {
		return nil, err
	}
	dt := dataType(dtByte)
	if dt == nullType {
		if _, err := d.readByte(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	flag, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if flag == valueFlagNull {
		return nil, nil
	}
	return d.readValue(dt, flag)
}

func (d *GraphBinaryDeserializer) readValue(dt dataType, flag byte) (interface{}, error) {
	switch dt {
	case intType:
		return d.readInt32()
	case longType:
		return d.readInt64()
	case stringType:
		return d.readString()
	case doubleType:
		if d.err != nil {
			return nil, d.err
		}
		if _, err := io.ReadFull(d.r, d.buf[:8]); err != nil {
			d.err = err
			return nil, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(d.buf[:8])), nil
	case floatType:
		if d.err != nil {
			return nil, d.err
		}
		if _, err := io.ReadFull(d.r, d.buf[:4]); err != nil {
			d.err = err
			return nil, err
		}
		return math.Float32frombits(binary.BigEndian.Uint32(d.buf[:4])), nil
	case booleanType:
		b, err := d.readByte()
		return b != 0, err
	case byteType:
		return d.readByte()
	case shortType:
		if d.err != nil {
			return nil, d.err
		}
		if _, err := io.ReadFull(d.r, d.buf[:2]); err != nil {
			d.err = err
			return nil, err
		}
		return int16(binary.BigEndian.Uint16(d.buf[:2])), nil
	case uuidType:
		buf, err := d.readBytes(16)
		if err != nil {
			return nil, err
		}
		id, err := uuid.FromBytes(buf)
		if err != nil {
			return nil, err
		}
		return id, nil
	case listType:
		return d.readList(flag == flagBulked)
	case setType:
		list, err := d.readList(flag == flagBulked)
		if err != nil {
			return nil, err
		}
		return NewSimpleSet(list.([]interface{})...), nil
	case mapType:
		return d.readMap()
	case vertexType:
		return d.readVertex(true)
	case edgeType:
		return d.readEdge()
	case pathType:
		return d.readPath()
	case propertyType:
		return d.readProperty()
	case vertexPropertyType:
		return d.readVertexProperty()
	case bigIntegerType:
		return d.readBigInt()
	case bigDecimalType:
		return d.readBigDecimal()
	case datetimeType:
		return d.readDateTime()
	case durationType:
		return d.readDuration()
	case markerType:
		b, err := d.readByte()
		if err != nil {
			return nil, err
		}
		return Of(b)
	case byteBuffer:
		return d.readByteBuffer()
	case tType, directionType, mergeType, gTypeType:
		return d.readEnum()
	default:
		return nil, newError(err0408GetSerializerToReadUnknownTypeError, dt)
	}
}

func (d *GraphBinaryDeserializer) readString() (string, error) {
	length, err := d.readInt32()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	buf, err := d.readBytes(int(length))
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (d *GraphBinaryDeserializer) readList(bulked bool) (interface{}, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	list := make([]interface{}, 0, length)
	for i := int32(0); i < length; i++ {
		val, err := d.ReadFullyQualified()
		if err != nil {
			return nil, err
		}
		if bulked {
			bulk, err := d.readInt64()
			if err != nil {
				return nil, err
			}
			for j := int64(0); j < bulk; j++ {
				list = append(list, val)
			}
		} else {
			list = append(list, val)
		}
	}
	return list, nil
}

func (d *GraphBinaryDeserializer) readMap() (interface{}, error) {
	length, err := d.readUint32()
	if err != nil {
		return nil, err
	}
	m := make(map[interface{}]interface{}, length)
	for i := uint32(0); i < length; i++ {
		key, err := d.ReadFullyQualified()
		if err != nil {
			return nil, err
		}
		val, err := d.ReadFullyQualified()
		if err != nil {
			return nil, err
		}
		if key == nil {
			m[nil] = val
		} else if reflect.TypeOf(key).Comparable() {
			m[key] = val
		} else if reflect.TypeOf(key).Kind() == reflect.Map {
			m[&key] = val
		} else {
			m[fmt.Sprint(key)] = val
		}
	}
	return m, nil
}

func (d *GraphBinaryDeserializer) readVertex(withProps bool) (*Vertex, error) {
	id, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	labels, err := d.readList(false)
	if err != nil {
		return nil, err
	}
	labelSlice, ok := labels.([]interface{})
	if !ok || len(labelSlice) == 0 {
		return nil, newError(err0404ReadNullTypeError)
	}
	label, ok := labelSlice[0].(string)
	if !ok {
		return nil, newError(err0404ReadNullTypeError)
	}
	v := &Vertex{Element: Element{Id: id, Label: label}}
	if withProps {
		props, err := d.ReadFullyQualified()
		if err != nil {
			return nil, err
		}
		v.Properties = make([]interface{}, 0)
		if props != nil {
			v.Properties = props
		}
	}
	return v, nil
}

func (d *GraphBinaryDeserializer) readEdge() (*Edge, error) {
	id, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	labels, err := d.readList(false)
	if err != nil {
		return nil, err
	}
	labelSlice, ok := labels.([]interface{})
	if !ok || len(labelSlice) == 0 {
		return nil, newError(err0404ReadNullTypeError)
	}
	label, ok := labelSlice[0].(string)
	if !ok {
		return nil, newError(err0404ReadNullTypeError)
	}
	inV, err := d.readVertex(false)
	if err != nil {
		return nil, err
	}
	outV, err := d.readVertex(false)
	if err != nil {
		return nil, err
	}
	if _, err := d.readBytes(2); err != nil {
		return nil, err
	}
	props, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	e := &Edge{
		Element: Element{Id: id, Label: label},
		InV:     *inV,
		OutV:    *outV,
	}
	e.Properties = make([]interface{}, 0)
	if props != nil {
		e.Properties = props
	}
	return e, nil
}

func (d *GraphBinaryDeserializer) readPath() (*Path, error) {
	labels, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	objects, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	objectSlice, ok := objects.([]interface{})
	if !ok {
		return nil, newError(err0404ReadNullTypeError)
	}
	path := &Path{Objects: objectSlice}
	if labels != nil {
		labelSlice, ok := labels.([]interface{})
		if !ok {
			return nil, newError(err0404ReadNullTypeError)
		}
		for _, l := range labelSlice {
			set, ok := l.(*SimpleSet)
			if !ok {
				return nil, newError(err0404ReadNullTypeError)
			}
			path.Labels = append(path.Labels, set)
		}
	}
	return path, nil
}

func (d *GraphBinaryDeserializer) readProperty() (*Property, error) {
	key, err := d.readString()
	if err != nil {
		return nil, err
	}
	value, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	if _, err := d.readBytes(2); err != nil {
		return nil, err
	}
	return &Property{Key: key, Value: value}, nil
}

func (d *GraphBinaryDeserializer) readVertexProperty() (*VertexProperty, error) {
	id, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	labels, err := d.readList(false)
	if err != nil {
		return nil, err
	}
	labelSlice, ok := labels.([]interface{})
	if !ok || len(labelSlice) == 0 {
		return nil, newError(err0404ReadNullTypeError)
	}
	label, ok := labelSlice[0].(string)
	if !ok {
		return nil, newError(err0404ReadNullTypeError)
	}
	value, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	if _, err := d.readBytes(2); err != nil {
		return nil, err
	}
	props, err := d.ReadFullyQualified()
	if err != nil {
		return nil, err
	}
	vp := &VertexProperty{
		Element: Element{Id: id, Label: label},
		Value:   value,
	}
	vp.Properties = make([]interface{}, 0)
	if props != nil {
		vp.Properties = props
	}
	return vp, nil
}

func (d *GraphBinaryDeserializer) readBigInt() (*big.Int, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return big.NewInt(0), nil
	}
	b, err := d.readBytes(int(length))
	if err != nil {
		return nil, err
	}
	bi := big.NewInt(0).SetBytes(b)
	if b[0]&0x80 != 0 {
		one := big.NewInt(1)
		bitLen := uint((len(b)*8)/8+1) * 8
		bi.Sub(bi, new(big.Int).Lsh(one, bitLen))
	}
	return bi, nil
}

func (d *GraphBinaryDeserializer) readBigDecimal() (*BigDecimal, error) {
	scale, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	unscaled, err := d.readBigInt()
	if err != nil {
		return nil, err
	}
	return &BigDecimal{Scale: scale, UnscaledValue: unscaled}, nil
}

func (d *GraphBinaryDeserializer) readDateTime() (time.Time, error) {
	year, err := d.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	month, err := d.readByte()
	if err != nil {
		return time.Time{}, err
	}
	day, err := d.readByte()
	if err != nil {
		return time.Time{}, err
	}
	totalNS, err := d.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	offset, err := d.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	ns := totalNS % 1e9
	totalS := totalNS / 1e9
	s := totalS % 60
	totalM := totalS / 60
	m := totalM % 60
	h := totalM / 60
	return time.Date(int(year), time.Month(month), int(day), int(h), int(m), int(s), int(ns), GetTimezoneFromOffset(int(offset))), nil
}

func (d *GraphBinaryDeserializer) readDuration() (time.Duration, error) {
	seconds, err := d.readInt64()
	if err != nil {
		return 0, err
	}
	nanos, err := d.readInt32()
	if err != nil {
		return 0, err
	}
	return time.Duration(seconds*int64(time.Second) + int64(nanos)), nil
}

func (d *GraphBinaryDeserializer) readByteBuffer() (*ByteBuffer, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	data, err := d.readBytes(int(length))
	if err != nil {
		return nil, err
	}
	return &ByteBuffer{Data: data}, nil
}

func (d *GraphBinaryDeserializer) readEnum() (string, error) {
	if _, err := d.readByte(); err != nil { // type code (string)
		return "", err
	}
	if _, err := d.readByte(); err != nil { // null flag
		return "", err
	}
	return d.readString()
}

// ReadStatus reads the response status after the EndOfStream marker.
// Returns the status code, message, exception string, and any error encountered.
// This should be called after ReadFullyQualified() returns an EndOfStream marker.
func (d *GraphBinaryDeserializer) ReadStatus() (code uint32, message string, exception string, err error) {
	code, err = d.readUint32()
	if err != nil {
		return 0, "", "", err
	}
	flag, err := d.readByte()
	if err != nil {
		return code, "", "", err
	}
	if flag != valueFlagNull {
		message, err = d.readString()
		if err != nil {
			return code, "", "", err
		}
	}
	flag, err = d.readByte()
	if err != nil {
		return code, message, "", err
	}
	if flag != valueFlagNull {
		exception, err = d.readString()
		if err != nil {
			return code, message, "", err
		}
	}
	return code, message, exception, nil
}
