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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// EWKB flag bits, which mark the PostGIS extended format that ISO WKB does not carry.
const (
	ewkbZFlag    uint32 = 0x80000000
	ewkbMFlag    uint32 = 0x40000000
	ewkbSridFlag uint32 = 0x20000000
)

// maxWkbCapacity bounds the capacity reserved up front for a count read from the input, so a
// hostile count cannot force a large allocation before the items are read.
const maxWkbCapacity = 1024

// wkbSource supplies the bytes of a WKB geometry. A reader takes the bytes it needs one piece at a
// time and stops where the geometry ends.
type wkbSource interface {
	readByte() (byte, error)
	readBytes(n int) ([]byte, error)
}

// wkbStreamSource reads WKB bytes from an io.Reader.
type wkbStreamSource struct {
	r io.Reader
}

func (s *wkbStreamSource) readByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(s.r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func (s *wkbStreamSource) readBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(s.r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// GeometryFromWKB reads one geometry from ISO WKB bytes, applying srid as given without
// validating the SRID itself.
//
// Each geometry header carries its own byte order marker, so both byte orders are read. EWKB, the
// M and ZM dimensions and unknown type codes are rejected. Input that ends inside the geometry,
// and input that carries bytes after it, are errors.
func GeometryFromWKB(wkb []byte, srid int32) (Geometry, error) {
	if len(wkb) == 0 {
		return nil, errors.New("WKB input cannot be empty")
	}
	reader := bytes.NewReader(wkb)
	g, err := readWkbGeometry(&wkbStreamSource{r: reader}, srid)
	if err != nil {
		return nil, wrapTruncatedWkbError(err, "truncated WKB input")
	}
	if reader.Len() > 0 {
		return nil, errors.New("unexpected trailing bytes in WKB input")
	}
	return g, nil
}

// wrapTruncatedWkbError adds geometry context to the end of input error of a WKB read, so that a
// caller sees which read ran out of bytes rather than a bare EOF. The result wraps the original
// error, so errors.Is still reports io.EOF or io.ErrUnexpectedEOF. Any other error is returned
// unchanged.
func wrapTruncatedWkbError(err error, context string) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%s: %w", context, err)
	}
	return err
}

// readWkbGeometry reads one geometry from src, leaving src positioned on the byte after it.
func readWkbGeometry(src wkbSource, srid int32) (Geometry, error) {
	return (&wkbReader{src: src, srid: srid}).readGeometry()
}

// wkbReader reads ISO WKB. The z dimension is read from the type code of the first geometry and
// every nested geometry must agree with it, and the collection nesting is counted as it is read.
type wkbReader struct {
	src    wkbSource
	srid   int32
	depth  int
	zKnown bool
	z      bool
}

func (r *wkbReader) readGeometry() (Geometry, error) {
	orderByte, err := r.src.readByte()
	if err != nil {
		return nil, err
	}
	var order binary.ByteOrder
	switch orderByte {
	case wkbBigEndian:
		order = binary.BigEndian
	case wkbLittleEndian:
		order = binary.LittleEndian
	default:
		return nil, fmt.Errorf("invalid WKB byte order: %d", orderByte)
	}

	typeCode, err := r.readUint32(order)
	if err != nil {
		return nil, err
	}
	if typeCode&(ewkbZFlag|ewkbMFlag|ewkbSridFlag) != 0 {
		return nil, fmt.Errorf("EWKB format detected (type code 0x%08X); ISO WKB is expected."+
			" EWKB with Z, M, or SRID flags is not supported", typeCode)
	}
	if typeCode >= 2001 && typeCode <= 2007 {
		return nil, fmt.Errorf("M dimension not supported (type code %d)", typeCode)
	}
	if typeCode >= 3001 && typeCode <= 3007 {
		return nil, fmt.Errorf("ZM dimension not supported (type code %d)", typeCode)
	}

	var baseType uint32
	var z bool
	switch {
	case typeCode >= 1001 && typeCode <= 1007:
		baseType = typeCode - wkbZOffset
		z = true
	case typeCode >= 1 && typeCode <= 7:
		baseType = typeCode
	default:
		return nil, fmt.Errorf("unknown WKB type code: %d", typeCode)
	}

	if !r.zKnown {
		r.zKnown = true
		r.z = z
	} else if r.z != z {
		return nil, errors.New("mixed z dimensions in WKB: some geometries have z, some do not")
	}

	switch baseType {
	case wkbTypePoint:
		return r.readPoint(z, order)
	case wkbTypeLineString:
		return r.readLineString(z, order)
	case wkbTypePolygon:
		return r.readPolygon(z, order)
	case wkbTypeMultiPoint:
		return r.readMultiPoint(z, order)
	case wkbTypeMultiLineString:
		return r.readMultiLineString(z, order)
	case wkbTypeMultiPolygon:
		return r.readMultiPolygon(z, order)
	case wkbTypeGeometryCollection:
		return r.readGeometryCollection(z, order)
	default:
		return nil, fmt.Errorf("unknown WKB type: %d", baseType)
	}
}

// readPoint reads one point. NaN x and y encode an empty point, so such a 3D point reads as an
// empty point with a Z tag and its z ordinate is read and discarded.
func (r *wkbReader) readPoint(z bool, order binary.ByteOrder) (*Point, error) {
	x, err := r.readFloat64(order)
	if err != nil {
		return nil, err
	}
	y, err := r.readFloat64(order)
	if err != nil {
		return nil, err
	}

	if math.IsNaN(x) && math.IsNaN(y) {
		if z {
			if _, err := r.readFloat64(order); err != nil {
				return nil, err
			}
		}
		return newEmptyPointWithSrid(r.srid, z), nil
	}

	if z {
		zValue, err := r.readFloat64(order)
		if err != nil {
			return nil, err
		}
		return NewPointZWithSrid(x, y, zValue, r.srid)
	}
	return NewPointWithSrid(x, y, r.srid)
}

// readVertex reads one coordinate as a vertex of a line string or a ring. A vertex takes a real
// coordinate, so NaN, which encodes an empty point, is malformed here.
func (r *wkbReader) readVertex(z bool, order binary.ByteOrder, context string) (*Point, error) {
	x, err := r.readFloat64(order)
	if err != nil {
		return nil, err
	}
	y, err := r.readFloat64(order)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(x) && math.IsNaN(y) {
		return nil, fmt.Errorf("a %s cannot be an empty point; a vertex must be a real coordinate", context)
	}
	if z {
		zValue, err := r.readFloat64(order)
		if err != nil {
			return nil, err
		}
		return NewPointZWithSrid(x, y, zValue, r.srid)
	}
	return NewPointWithSrid(x, y, r.srid)
}

func (r *wkbReader) readLineString(z bool, order binary.ByteOrder) (*LineString, error) {
	numPoints, err := r.readCount(order, "point count")
	if err != nil {
		return nil, err
	}
	if numPoints == 0 {
		return newEmptyLineStringWithSrid(r.srid, z), nil
	}
	if numPoints == 1 {
		return nil, errors.New("a line string must have at least 2 points, got 1")
	}
	points, err := r.readVertices(numPoints, z, order, "linestring vertex")
	if err != nil {
		return nil, err
	}
	return newLineStringWithSrid(points, r.srid, z)
}

func (r *wkbReader) readPolygon(z bool, order binary.ByteOrder) (*Polygon, error) {
	numRings, err := r.readCount(order, "ring count")
	if err != nil {
		return nil, err
	}
	if numRings == 0 {
		return newEmptyPolygonWithSrid(r.srid, z), nil
	}

	rings := make([]*LineString, 0, initialWkbCapacity(numRings))
	for i := 0; i < numRings; i++ {
		numPoints, err := r.readCount(order, "point count for ring")
		if err != nil {
			return nil, err
		}
		if numPoints < 4 {
			return nil, fmt.Errorf("a polygon ring must have at least 4 points, got %d", numPoints)
		}
		points, err := r.readVertices(numPoints, z, order, "ring vertex")
		if err != nil {
			return nil, err
		}
		ring, err := newLineStringWithSrid(points, r.srid, z)
		if err != nil {
			return nil, err
		}
		context := "hole ring"
		if i == 0 {
			context = "shell ring"
		}
		if err := validateRing(ring, context); err != nil {
			return nil, err
		}
		rings = append(rings, ring)
	}

	return newPolygonWithSrid(rings[0], rings[1:], r.srid, z)
}

// readVertices reads the coordinates of a line string or a ring. The list grows as the points are
// read, so a count larger than the input cannot force a large allocation.
func (r *wkbReader) readVertices(count int, z bool, order binary.ByteOrder, context string) ([]*Point, error) {
	points := make([]*Point, 0, initialWkbCapacity(count))
	for i := 0; i < count; i++ {
		p, err := r.readVertex(z, order, context)
		if err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, nil
}

func (r *wkbReader) readMultiPoint(z bool, order binary.ByteOrder) (*MultiPoint, error) {
	count, err := r.enterCollection(order)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		r.depth--
		return newEmptyMultiPointWithSrid(r.srid, z), nil
	}

	points := make([]*Point, 0, initialWkbCapacity(count))
	for i := 0; i < count; i++ {
		g, err := r.readGeometry()
		if err != nil {
			return nil, err
		}
		p, ok := g.(*Point)
		if !ok {
			return nil, fmt.Errorf("expected a point in a multi-point, got %s", g.wktTypeName())
		}
		points = append(points, p)
	}

	r.depth--
	return newMultiPointWithSrid(points, r.srid, z)
}

func (r *wkbReader) readMultiLineString(z bool, order binary.ByteOrder) (*MultiLineString, error) {
	count, err := r.enterCollection(order)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		r.depth--
		return newEmptyMultiLineStringWithSrid(r.srid, z), nil
	}

	lineStrings := make([]*LineString, 0, initialWkbCapacity(count))
	for i := 0; i < count; i++ {
		g, err := r.readGeometry()
		if err != nil {
			return nil, err
		}
		ls, ok := g.(*LineString)
		if !ok {
			return nil, fmt.Errorf("expected a line string in a multi-line string, got %s", g.wktTypeName())
		}
		lineStrings = append(lineStrings, ls)
	}

	r.depth--
	return newMultiLineStringWithSrid(lineStrings, r.srid, z)
}

func (r *wkbReader) readMultiPolygon(z bool, order binary.ByteOrder) (*MultiPolygon, error) {
	count, err := r.enterCollection(order)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		r.depth--
		return newEmptyMultiPolygonWithSrid(r.srid, z), nil
	}

	polygons := make([]*Polygon, 0, initialWkbCapacity(count))
	for i := 0; i < count; i++ {
		g, err := r.readGeometry()
		if err != nil {
			return nil, err
		}
		poly, ok := g.(*Polygon)
		if !ok {
			return nil, fmt.Errorf("expected a polygon in a multi-polygon, got %s", g.wktTypeName())
		}
		polygons = append(polygons, poly)
	}

	r.depth--
	return newMultiPolygonWithSrid(polygons, r.srid, z)
}

func (r *wkbReader) readGeometryCollection(z bool, order binary.ByteOrder) (*GeometryCollection, error) {
	count, err := r.enterCollection(order)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		r.depth--
		return newEmptyGeometryCollectionWithSrid(r.srid, z), nil
	}

	geometries := make([]Geometry, 0, initialWkbCapacity(count))
	for i := 0; i < count; i++ {
		g, err := r.readGeometry()
		if err != nil {
			return nil, err
		}
		geometries = append(geometries, g)
	}

	r.depth--
	return newGeometryCollectionWithSrid(geometries, r.srid, z)
}

// enterCollection counts one more level of nesting and reads the part count of a collection.
func (r *wkbReader) enterCollection(order binary.ByteOrder) (int, error) {
	r.depth++
	if err := validateNestingDepth(r.depth); err != nil {
		return 0, err
	}
	return r.readCount(order, "geometry count")
}

// readCount reads a count of points, rings or parts. The count is unsigned on the wire, so a
// value that does not fit a signed 32-bit integer is malformed.
func (r *wkbReader) readCount(order binary.ByteOrder, context string) (int, error) {
	value, err := r.readUint32(order)
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("invalid %s: %d", context, value)
	}
	return int(value), nil
}

func (r *wkbReader) readUint32(order binary.ByteOrder) (uint32, error) {
	b, err := r.src.readBytes(4)
	if err != nil {
		return 0, err
	}
	return order.Uint32(b), nil
}

func (r *wkbReader) readFloat64(order binary.ByteOrder) (float64, error) {
	b, err := r.src.readBytes(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(order.Uint64(b)), nil
}

func initialWkbCapacity(count int) int {
	if count > maxWkbCapacity {
		return maxWkbCapacity
	}
	return count
}
