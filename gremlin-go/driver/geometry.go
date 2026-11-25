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
	"errors"
	"fmt"
	"strings"
)

// model is the coordinate interpretation model of a Geometry.
type model string

type models struct {
	// Cartesian is planar, with math in coordinate units. SRID 0.
	Cartesian model
	// Geographic is longitude and latitude on the WGS84 earth surface. SRID 4326.
	Geographic model
	// Other is any other spatial reference system, such as a map projection. It is reported by
	// the Model method of a geometry whose SRID is neither 0 nor 4326, and is never accepted by
	// a constructor: a geometry in another coordinate system is meaningless without knowing
	// which one, and the SRID says exactly that.
	Other model
}

// Model holds the coordinate interpretation models of a Geometry.
var Model = models{
	Cartesian:  "CARTESIAN",
	Geographic: "GEOGRAPHIC",
	Other:      "OTHER",
}

const (
	sridCartesian  int32 = 0
	sridGeographic int32 = 4326

	// maxGeometryNestingDepth is the deepest collection nesting a geometry may carry.
	maxGeometryNestingDepth = 32
)

// Geometry is a geospatial value. Geometry values are immutable: their fields are unexported and
// their part accessors hand out copies, so a value cannot be changed once it is built.
//
// The kinds are Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon and
// GeometryCollection. Only those types implement Geometry, which carries unexported methods.
//
// A geometry holds slices, so it cannot be compared with == and cannot be used as a map key.
// Use Equal to compare two geometries.
type Geometry interface {
	// Srid returns the spatial reference ID.
	Srid() int32
	// Model returns the coordinate interpretation model derived from the SRID.
	Model() model
	// HasZ reports whether the coordinates carry a z ordinate.
	HasZ() bool
	// IsEmpty reports whether this geometry holds no coordinates. A collection is empty when
	// all of its parts are empty.
	IsEmpty() bool
	// WKB returns the ISO WKB encoding in big-endian byte order.
	WKB() []byte
	// String returns the ISO WKT text, without an SRID prefix.
	String() string
	// WKT returns the ISO WKT text, without an SRID prefix. It is the same text as String.
	WKT() string
	// EWKT returns the ISO WKT text with an "SRID=<srid>;" prefix, which is always written, so
	// an SRID of 0 appears as "SRID=0;".
	EWKT() string
	// Equal reports whether other is of the same kind and holds the same SRID, z dimension and
	// coordinates. Coordinates are compared numerically, so -0.0 equals 0.0. A nil other, which
	// includes a nil pointer of a geometry type, is never equal.
	Equal(other Geometry) bool

	// wkbTypeCode returns the ISO WKB code of this kind, without the 1000 a z dimension adds.
	wkbTypeCode() uint32
	// wktTypeName returns the WKT keyword of this kind.
	wktTypeName() string
	// hasNoParts reports whether this geometry holds no parts of its own: no coordinate for a
	// point, no points for a line string, no rings for a polygon, no elements for a collection.
	// This is the structural test the writers use to choose between "X EMPTY" and a
	// parenthesized body, so that WKT and WKB of one value describe the same parts. It differs
	// from IsEmpty, which is deep: a collection whose parts are all empty is empty, but it
	// still has parts.
	hasNoParts() bool
	// writeWktBody appends the coordinates or parts that go inside the parentheses.
	writeWktBody(sb *strings.Builder)
	// writeWkbBody appends the WKB bytes that follow the byte order marker and the type code.
	writeWkbBody(buf *bytes.Buffer)
}

// Curve is a geometry whose extent is one-dimensional. LineString is the only curve kind.
type Curve interface {
	Geometry
	isCurve()
}

// Surface is a geometry whose extent is two-dimensional. Polygon is the only surface kind.
type Surface interface {
	Geometry
	isSurface()
}

// MultiCurve is a collection of curves. MultiLineString is the only multi-curve kind.
type MultiCurve interface {
	Geometry
	isMultiCurve()
}

// MultiSurface is a collection of surfaces. MultiPolygon is the only multi-surface kind.
type MultiSurface interface {
	Geometry
	isMultiSurface()
}

// geometryBase holds the SRID and the z dimension flag that every geometry carries.
type geometryBase struct {
	srid int32
	hasZ bool
}

// Srid returns the spatial reference ID of this geometry.
func (g geometryBase) Srid() int32 {
	return g.srid
}

// Model returns the coordinate interpretation model derived from the SRID: Model.Cartesian for
// SRID 0, Model.Geographic for SRID 4326 and Model.Other for every other SRID.
func (g geometryBase) Model() model {
	return modelForSrid(g.srid)
}

// HasZ reports whether the coordinates of this geometry carry a z ordinate.
func (g geometryBase) HasZ() bool {
	return g.hasZ
}

func modelForSrid(srid int32) model {
	switch srid {
	case sridCartesian:
		return Model.Cartesian
	case sridGeographic:
		return Model.Geographic
	default:
		return Model.Other
	}
}

func sridForModel(m model) (int32, error) {
	switch m {
	case Model.Cartesian:
		return sridCartesian, nil
	case Model.Geographic:
		return sridGeographic, nil
	case Model.Other:
		return 0, errors.New("Model.Other cannot be used in a constructor;" +
			" use a constructor that takes an explicit SRID")
	default:
		return 0, fmt.Errorf("unknown model: %q", string(m))
	}
}

func equalPoints(a, b []*Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func equalLineStrings(a, b []*LineString) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func equalPolygons(a, b []*Polygon) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func equalGeometries(a, b []Geometry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func copyPoints(points []*Point) []*Point {
	if len(points) == 0 {
		return nil
	}
	out := make([]*Point, len(points))
	copy(out, points)
	return out
}

func copyLineStrings(lineStrings []*LineString) []*LineString {
	if len(lineStrings) == 0 {
		return nil
	}
	out := make([]*LineString, len(lineStrings))
	copy(out, lineStrings)
	return out
}

func copyPolygons(polygons []*Polygon) []*Polygon {
	if len(polygons) == 0 {
		return nil
	}
	out := make([]*Polygon, len(polygons))
	copy(out, polygons)
	return out
}

func copyGeometries(geometries []Geometry) []Geometry {
	if len(geometries) == 0 {
		return nil
	}
	out := make([]Geometry, len(geometries))
	copy(out, geometries)
	return out
}

// nilPartError reports a nil part of a geometry. A nil part would panic when it is read, so it is
// rejected on construction.
func nilPartError(context string, index int) error {
	return fmt.Errorf("%s element at index %d cannot be nil", context, index)
}

// isNilGeometry reports whether g carries no geometry. A Geometry holds both a type and a
// pointer, so a nil pointer of a geometry type, such as (*Point)(nil), is not a nil interface
// and has to be checked against each kind. Only the kinds of this package implement Geometry.
func isNilGeometry(g Geometry) bool {
	if g == nil {
		return true
	}
	switch v := g.(type) {
	case *Point:
		return v == nil
	case *LineString:
		return v == nil
	case *Polygon:
		return v == nil
	case *MultiPoint:
		return v == nil
	case *MultiLineString:
		return v == nil
	case *MultiPolygon:
		return v == nil
	case *GeometryCollection:
		return v == nil
	default:
		return false
	}
}
