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
	"math"
)

const (
	wkbBigEndian    byte = 0x00
	wkbLittleEndian byte = 0x01

	// wkbZOffset is added to a type code to mark a 3D geometry in ISO WKB.
	wkbZOffset uint32 = 1000

	// wkbNaNBits is the quiet NaN that stands for a missing ordinate of an empty point.
	wkbNaNBits uint64 = 0x7ff8000000000000

	wkbTypePoint              uint32 = 1
	wkbTypeLineString         uint32 = 2
	wkbTypePolygon            uint32 = 3
	wkbTypeMultiPoint         uint32 = 4
	wkbTypeMultiLineString    uint32 = 5
	wkbTypeMultiPolygon       uint32 = 6
	wkbTypeGeometryCollection uint32 = 7
)

// WKB returns the ISO WKB encoding of this point, in big-endian byte order.
func (p *Point) WKB() []byte {
	return geometryWkb(p)
}

// WKB returns the ISO WKB encoding of this line string, in big-endian byte order.
func (ls *LineString) WKB() []byte {
	return geometryWkb(ls)
}

// WKB returns the ISO WKB encoding of this polygon, in big-endian byte order.
func (poly *Polygon) WKB() []byte {
	return geometryWkb(poly)
}

// WKB returns the ISO WKB encoding of this multi-point, in big-endian byte order.
func (mp *MultiPoint) WKB() []byte {
	return geometryWkb(mp)
}

// WKB returns the ISO WKB encoding of this multi-line string, in big-endian byte order.
func (mls *MultiLineString) WKB() []byte {
	return geometryWkb(mls)
}

// WKB returns the ISO WKB encoding of this multi-polygon, in big-endian byte order.
func (mpoly *MultiPolygon) WKB() []byte {
	return geometryWkb(mpoly)
}

// WKB returns the ISO WKB encoding of this collection, in big-endian byte order.
func (gc *GeometryCollection) WKB() []byte {
	return geometryWkb(gc)
}

func geometryWkb(g Geometry) []byte {
	buf := &bytes.Buffer{}
	writeWkbGeometry(buf, g)
	return buf.Bytes()
}

// writeWkbGeometry appends the byte order marker, the type code and the body of one geometry.
func writeWkbGeometry(buf *bytes.Buffer, g Geometry) {
	buf.WriteByte(wkbBigEndian)
	typeCode := g.wkbTypeCode()
	if g.HasZ() {
		typeCode += wkbZOffset
	}
	writeWkbUint32(buf, typeCode)
	g.writeWkbBody(buf)
}

func writeWkbUint32(buf *bytes.Buffer, value uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], value)
	buf.Write(b[:])
}

func writeWkbUint64(buf *bytes.Buffer, value uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	buf.Write(b[:])
}

func writeWkbFloat64(buf *bytes.Buffer, value float64) {
	writeWkbUint64(buf, math.Float64bits(value))
}

func (p *Point) wkbTypeCode() uint32 {
	return wkbTypePoint
}

func (p *Point) writeWkbBody(buf *bytes.Buffer) {
	p.writeWkbCoordinate(buf, p.hasZ)
}

// writeWkbCoordinate appends x and y, and z when the enclosing geometry is 3D. An empty point
// holds no coordinate, which ISO WKB encodes as NaN ordinates.
func (p *Point) writeWkbCoordinate(buf *bytes.Buffer, hasZ bool) {
	if p.empty {
		writeWkbUint64(buf, wkbNaNBits)
		writeWkbUint64(buf, wkbNaNBits)
		if hasZ {
			writeWkbUint64(buf, wkbNaNBits)
		}
		return
	}
	writeWkbFloat64(buf, p.x)
	writeWkbFloat64(buf, p.y)
	if hasZ {
		writeWkbFloat64(buf, p.z)
	}
}

func (ls *LineString) wkbTypeCode() uint32 {
	return wkbTypeLineString
}

func (ls *LineString) writeWkbBody(buf *bytes.Buffer) {
	writeWkbPoints(buf, ls)
}

// writeWkbPoints appends the point count of a line string or ring, followed by its coordinates.
func writeWkbPoints(buf *bytes.Buffer, ls *LineString) {
	writeWkbUint32(buf, uint32(len(ls.points)))
	for _, p := range ls.points {
		p.writeWkbCoordinate(buf, ls.hasZ)
	}
}

func (poly *Polygon) wkbTypeCode() uint32 {
	return wkbTypePolygon
}

func (poly *Polygon) writeWkbBody(buf *bytes.Buffer) {
	if poly.hasNoParts() {
		writeWkbUint32(buf, 0)
		return
	}
	writeWkbUint32(buf, uint32(1+len(poly.holes)))
	writeWkbPoints(buf, poly.shellRing())
	for _, hole := range poly.holes {
		writeWkbPoints(buf, hole)
	}
}

func (mp *MultiPoint) wkbTypeCode() uint32 {
	return wkbTypeMultiPoint
}

func (mp *MultiPoint) writeWkbBody(buf *bytes.Buffer) {
	writeWkbUint32(buf, uint32(len(mp.points)))
	for _, p := range mp.points {
		writeWkbGeometry(buf, p)
	}
}

func (mls *MultiLineString) wkbTypeCode() uint32 {
	return wkbTypeMultiLineString
}

func (mls *MultiLineString) writeWkbBody(buf *bytes.Buffer) {
	writeWkbUint32(buf, uint32(len(mls.lineStrings)))
	for _, ls := range mls.lineStrings {
		writeWkbGeometry(buf, ls)
	}
}

func (mpoly *MultiPolygon) wkbTypeCode() uint32 {
	return wkbTypeMultiPolygon
}

func (mpoly *MultiPolygon) writeWkbBody(buf *bytes.Buffer) {
	writeWkbUint32(buf, uint32(len(mpoly.polygons)))
	for _, poly := range mpoly.polygons {
		writeWkbGeometry(buf, poly)
	}
}

func (gc *GeometryCollection) wkbTypeCode() uint32 {
	return wkbTypeGeometryCollection
}

func (gc *GeometryCollection) writeWkbBody(buf *bytes.Buffer) {
	writeWkbUint32(buf, uint32(len(gc.geometries)))
	for _, g := range gc.geometries {
		writeWkbGeometry(buf, g)
	}
}
