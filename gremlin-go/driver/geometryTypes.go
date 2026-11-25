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
	"errors"
	"math"
)

// Point is a single location in 2D or 3D space.
type Point struct {
	geometryBase
	x     float64
	y     float64
	z     float64
	empty bool
}

// LineString is a sequence of connected line segments.
type LineString struct {
	geometryBase
	points []*Point
}

// Polygon is an area enclosed by a shell ring, with optional hole rings.
type Polygon struct {
	geometryBase
	shell *LineString
	holes []*LineString
}

// MultiPoint is a collection of points.
type MultiPoint struct {
	geometryBase
	points []*Point
}

// MultiLineString is a collection of line strings.
type MultiLineString struct {
	geometryBase
	lineStrings []*LineString
}

// MultiPolygon is a collection of polygons.
type MultiPolygon struct {
	geometryBase
	polygons []*Polygon
}

// GeometryCollection is a collection of geometries of any kind.
type GeometryCollection struct {
	geometryBase
	geometries []Geometry
}

func (ls *LineString) isCurve() {}

func (poly *Polygon) isSurface() {}

func (mls *MultiLineString) isMultiCurve() {}

func (mpoly *MultiPolygon) isMultiSurface() {}

// ------------------------------------------------------------------------
// Constructors
// ------------------------------------------------------------------------

// NewPoint creates a 2D point with the given coordinate model, which must be Model.Cartesian or
// Model.Geographic. Under Model.Geographic, x is a longitude in [-180, 180] and y is a latitude
// in [-90, 90]. Use NewPointWithSrid for a point in another spatial reference system.
func NewPoint(x, y float64, m model) (*Point, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return NewPointWithSrid(x, y, srid)
}

// NewPointZ creates a 3D point with the given coordinate model, which must be Model.Cartesian or
// Model.Geographic.
func NewPointZ(x, y, z float64, m model) (*Point, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return NewPointZWithSrid(x, y, z, srid)
}

// NewEmptyPoint creates an empty 2D point with the given coordinate model, which must be
// Model.Cartesian or Model.Geographic.
func NewEmptyPoint(m model) (*Point, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyPointWithSrid(srid, false), nil
}

// NewEmptyPointWithSrid creates an empty 2D point with an explicit SRID, which is applied as
// given and is not itself validated. An SRID of 0 gives Model.Cartesian, 4326 gives
// Model.Geographic and any other SRID gives Model.Other.
func NewEmptyPointWithSrid(srid int32) *Point {
	return newEmptyPointWithSrid(srid, false)
}

// NewPointWithSrid creates a 2D point with an explicit SRID, which is applied as given and is
// not itself validated. An SRID of 0 gives Model.Cartesian, 4326 gives Model.Geographic and any
// other SRID gives Model.Other. The SRID of every other kind comes from its parts, so a point is
// where an SRID enters a geometry.
func NewPointWithSrid(x, y float64, srid int32) (*Point, error) {
	if err := validateX(x, srid); err != nil {
		return nil, err
	}
	if err := validateY(y, srid); err != nil {
		return nil, err
	}
	return &Point{geometryBase: geometryBase{srid: srid}, x: x, y: y, z: math.NaN()}, nil
}

// NewPointZWithSrid creates a 3D point with an explicit SRID, which is applied as given and is
// not itself validated.
func NewPointZWithSrid(x, y, z float64, srid int32) (*Point, error) {
	if err := validateX(x, srid); err != nil {
		return nil, err
	}
	if err := validateY(y, srid); err != nil {
		return nil, err
	}
	if err := validateZ(z); err != nil {
		return nil, err
	}
	return &Point{geometryBase: geometryBase{srid: srid, hasZ: true}, x: x, y: y, z: z}, nil
}

// NewLineString creates a line string from at least 2 points. The points must all carry the same
// SRID and the same z dimension, and none of them may be empty. Use NewEmptyLineString for an
// empty line string.
func NewLineString(points ...*Point) (*LineString, error) {
	if len(points) == 0 {
		return nil, errors.New("a line string must have at least 2 points, got 0;" +
			" use NewEmptyLineString for an empty line string")
	}
	if len(points) == 1 {
		return nil, errors.New("a line string must have at least 2 points, got 1")
	}
	if err := validateVertex(points[0], "line string", 0); err != nil {
		return nil, err
	}
	return newLineStringWithSrid(copyPoints(points), points[0].srid, points[0].hasZ)
}

// NewEmptyLineString creates an empty line string with the given coordinate model, which must be
// Model.Cartesian or Model.Geographic.
func NewEmptyLineString(m model) (*LineString, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyLineStringWithSrid(srid, false), nil
}

// NewEmptyLineStringWithSrid creates an empty 2D line string with an explicit SRID, which is
// applied as given and is not itself validated. An SRID of 0 gives Model.Cartesian, 4326 gives
// Model.Geographic and any other SRID gives Model.Other.
func NewEmptyLineStringWithSrid(srid int32) *LineString {
	return newEmptyLineStringWithSrid(srid, false)
}

// NewPolygon creates a polygon from a shell ring and optional hole rings. Every ring must be
// closed, which means its first and last points are equal, and must hold at least 4 points. The
// rings must all carry the same SRID and the same z dimension. A hole ring must not be empty,
// and an empty shell is only valid for a polygon with no holes.
func NewPolygon(shell *LineString, holes ...*LineString) (*Polygon, error) {
	if shell == nil {
		return nil, errors.New("the shell ring of a polygon cannot be nil")
	}
	return newPolygonWithSrid(shell, copyLineStrings(holes), shell.srid, shell.hasZ)
}

// NewEmptyPolygon creates an empty polygon with the given coordinate model, which must be
// Model.Cartesian or Model.Geographic.
func NewEmptyPolygon(m model) (*Polygon, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyPolygonWithSrid(srid, false), nil
}

// NewEmptyPolygonWithSrid creates an empty 2D polygon with an explicit SRID, which is applied as
// given and is not itself validated. An SRID of 0 gives Model.Cartesian, 4326 gives
// Model.Geographic and any other SRID gives Model.Other.
func NewEmptyPolygonWithSrid(srid int32) *Polygon {
	return newEmptyPolygonWithSrid(srid, false)
}

// NewMultiPoint creates a multi-point from at least 1 point. The points must all carry the same
// SRID and the same z dimension, and any of them may be empty. Use NewEmptyMultiPoint for a
// multi-point with no parts.
func NewMultiPoint(points ...*Point) (*MultiPoint, error) {
	if len(points) == 0 {
		return nil, errors.New("a multi-point must have at least 1 point;" +
			" use NewEmptyMultiPoint for a multi-point with no parts")
	}
	if points[0] == nil {
		return nil, nilPartError("multi-point", 0)
	}
	srid, hasZ := points[0].srid, points[0].hasZ
	for i, p := range points {
		if p == nil {
			return nil, nilPartError("multi-point", i)
		}
		if err := validateSameSrid(srid, p.srid, "multi-point"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, p.hasZ, "multi-point"); err != nil {
			return nil, err
		}
	}
	return &MultiPoint{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, points: copyPoints(points)}, nil
}

// NewEmptyMultiPoint creates a multi-point with no parts and the given coordinate model, which
// must be Model.Cartesian or Model.Geographic.
func NewEmptyMultiPoint(m model) (*MultiPoint, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyMultiPointWithSrid(srid, false), nil
}

// NewEmptyMultiPointWithSrid creates a 2D multi-point with no parts and an explicit SRID, which
// is applied as given and is not itself validated. An SRID of 0 gives Model.Cartesian, 4326
// gives Model.Geographic and any other SRID gives Model.Other.
func NewEmptyMultiPointWithSrid(srid int32) *MultiPoint {
	return newEmptyMultiPointWithSrid(srid, false)
}

// NewMultiLineString creates a multi-line string from at least 1 line string. The line strings
// must all carry the same SRID and the same z dimension, and any of them may be empty. Use
// NewEmptyMultiLineString for a multi-line string with no parts.
func NewMultiLineString(lineStrings ...*LineString) (*MultiLineString, error) {
	if len(lineStrings) == 0 {
		return nil, errors.New("a multi-line string must have at least 1 line string;" +
			" use NewEmptyMultiLineString for a multi-line string with no parts")
	}
	if lineStrings[0] == nil {
		return nil, nilPartError("multi-line string", 0)
	}
	srid, hasZ := lineStrings[0].srid, lineStrings[0].hasZ
	for i, ls := range lineStrings {
		if ls == nil {
			return nil, nilPartError("multi-line string", i)
		}
		if err := validateSameSrid(srid, ls.srid, "multi-line string"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, ls.hasZ, "multi-line string"); err != nil {
			return nil, err
		}
	}
	return &MultiLineString{
		geometryBase: geometryBase{srid: srid, hasZ: hasZ},
		lineStrings:  copyLineStrings(lineStrings),
	}, nil
}

// NewEmptyMultiLineString creates a multi-line string with no parts and the given coordinate
// model, which must be Model.Cartesian or Model.Geographic.
func NewEmptyMultiLineString(m model) (*MultiLineString, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyMultiLineStringWithSrid(srid, false), nil
}

// NewEmptyMultiLineStringWithSrid creates a 2D multi-line string with no parts and an explicit
// SRID, which is applied as given and is not itself validated. An SRID of 0 gives
// Model.Cartesian, 4326 gives Model.Geographic and any other SRID gives Model.Other.
func NewEmptyMultiLineStringWithSrid(srid int32) *MultiLineString {
	return newEmptyMultiLineStringWithSrid(srid, false)
}

// NewMultiPolygon creates a multi-polygon from at least 1 polygon. The polygons must all carry
// the same SRID and the same z dimension, and any of them may be empty. Use NewEmptyMultiPolygon
// for a multi-polygon with no parts.
func NewMultiPolygon(polygons ...*Polygon) (*MultiPolygon, error) {
	if len(polygons) == 0 {
		return nil, errors.New("a multi-polygon must have at least 1 polygon;" +
			" use NewEmptyMultiPolygon for a multi-polygon with no parts")
	}
	if polygons[0] == nil {
		return nil, nilPartError("multi-polygon", 0)
	}
	srid, hasZ := polygons[0].srid, polygons[0].hasZ
	for i, poly := range polygons {
		if poly == nil {
			return nil, nilPartError("multi-polygon", i)
		}
		if err := validateSameSrid(srid, poly.srid, "multi-polygon"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, poly.hasZ, "multi-polygon"); err != nil {
			return nil, err
		}
	}
	return &MultiPolygon{
		geometryBase: geometryBase{srid: srid, hasZ: hasZ},
		polygons:     copyPolygons(polygons),
	}, nil
}

// NewEmptyMultiPolygon creates a multi-polygon with no parts and the given coordinate model,
// which must be Model.Cartesian or Model.Geographic.
func NewEmptyMultiPolygon(m model) (*MultiPolygon, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyMultiPolygonWithSrid(srid, false), nil
}

// NewEmptyMultiPolygonWithSrid creates a 2D multi-polygon with no parts and an explicit SRID,
// which is applied as given and is not itself validated. An SRID of 0 gives Model.Cartesian,
// 4326 gives Model.Geographic and any other SRID gives Model.Other.
func NewEmptyMultiPolygonWithSrid(srid int32) *MultiPolygon {
	return newEmptyMultiPolygonWithSrid(srid, false)
}

// NewGeometryCollection creates a geometry collection from at least 1 geometry. The geometries
// must all carry the same SRID and the same z dimension, and any of them may be empty or a
// collection of its own, as long as the nesting stays within 32 levels. Use
// NewEmptyGeometryCollection for a collection with no parts.
func NewGeometryCollection(geometries ...Geometry) (*GeometryCollection, error) {
	if len(geometries) == 0 {
		return nil, errors.New("a geometry collection must have at least 1 geometry;" +
			" use NewEmptyGeometryCollection for a geometry collection with no parts")
	}
	if isNilGeometry(geometries[0]) {
		return nil, nilPartError("geometry collection", 0)
	}
	srid, hasZ := geometries[0].Srid(), geometries[0].HasZ()
	maxChildDepth := 0
	for i, g := range geometries {
		if isNilGeometry(g) {
			return nil, nilPartError("geometry collection", i)
		}
		if err := validateSameSrid(srid, g.Srid(), "geometry collection"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, g.HasZ(), "geometry collection"); err != nil {
			return nil, err
		}
		if depth := collectionDepth(g); depth > maxChildDepth {
			maxChildDepth = depth
		}
	}
	// This level plus the deepest child must stay within the nesting limit.
	if err := validateNestingDepth(1 + maxChildDepth); err != nil {
		return nil, err
	}
	return &GeometryCollection{
		geometryBase: geometryBase{srid: srid, hasZ: hasZ},
		geometries:   copyGeometries(geometries),
	}, nil
}

// NewEmptyGeometryCollection creates a geometry collection with no parts and the given coordinate
// model, which must be Model.Cartesian or Model.Geographic.
func NewEmptyGeometryCollection(m model) (*GeometryCollection, error) {
	srid, err := sridForModel(m)
	if err != nil {
		return nil, err
	}
	return newEmptyGeometryCollectionWithSrid(srid, false), nil
}

// NewEmptyGeometryCollectionWithSrid creates a 2D geometry collection with no parts and an
// explicit SRID, which is applied as given and is not itself validated. An SRID of 0 gives
// Model.Cartesian, 4326 gives Model.Geographic and any other SRID gives Model.Other.
func NewEmptyGeometryCollectionWithSrid(srid int32) *GeometryCollection {
	return newEmptyGeometryCollectionWithSrid(srid, false)
}

// ------------------------------------------------------------------------
// Internal constructors, which take ownership of the slices they are given
// ------------------------------------------------------------------------

func newEmptyPointWithSrid(srid int32, hasZ bool) *Point {
	return &Point{
		geometryBase: geometryBase{srid: srid, hasZ: hasZ},
		x:            math.NaN(),
		y:            math.NaN(),
		z:            math.NaN(),
		empty:        true,
	}
}

func newEmptyLineStringWithSrid(srid int32, hasZ bool) *LineString {
	return &LineString{geometryBase: geometryBase{srid: srid, hasZ: hasZ}}
}

func newEmptyPolygonWithSrid(srid int32, hasZ bool) *Polygon {
	return &Polygon{
		geometryBase: geometryBase{srid: srid, hasZ: hasZ},
		shell:        newEmptyLineStringWithSrid(srid, hasZ),
	}
}

func newEmptyMultiPointWithSrid(srid int32, hasZ bool) *MultiPoint {
	return &MultiPoint{geometryBase: geometryBase{srid: srid, hasZ: hasZ}}
}

func newEmptyMultiLineStringWithSrid(srid int32, hasZ bool) *MultiLineString {
	return &MultiLineString{geometryBase: geometryBase{srid: srid, hasZ: hasZ}}
}

func newEmptyMultiPolygonWithSrid(srid int32, hasZ bool) *MultiPolygon {
	return &MultiPolygon{geometryBase: geometryBase{srid: srid, hasZ: hasZ}}
}

func newEmptyGeometryCollectionWithSrid(srid int32, hasZ bool) *GeometryCollection {
	return &GeometryCollection{geometryBase: geometryBase{srid: srid, hasZ: hasZ}}
}

func newLineStringWithSrid(points []*Point, srid int32, hasZ bool) (*LineString, error) {
	if len(points) == 0 {
		return newEmptyLineStringWithSrid(srid, hasZ), nil
	}
	if len(points) == 1 {
		return nil, errors.New("a line string must have at least 2 points, got 1")
	}
	for i, p := range points {
		if err := validateVertex(p, "line string", i); err != nil {
			return nil, err
		}
		if err := validateSameSrid(srid, p.srid, "line string"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, p.hasZ, "line string"); err != nil {
			return nil, err
		}
	}
	return &LineString{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, points: points}, nil
}

func newPolygonWithSrid(shell *LineString, holes []*LineString, srid int32, hasZ bool) (*Polygon, error) {
	if shell.hasNoParts() && len(holes) == 0 {
		return newEmptyPolygonWithSrid(srid, hasZ), nil
	}
	if !shell.hasNoParts() {
		if err := validateRing(shell, "shell ring"); err != nil {
			return nil, err
		}
		if err := validateSameSrid(srid, shell.srid, "polygon"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, shell.hasZ, "polygon"); err != nil {
			return nil, err
		}
	}
	if err := validateHoles(shell, holes, srid, hasZ); err != nil {
		return nil, err
	}
	return &Polygon{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, shell: shell, holes: holes}, nil
}

func newMultiPointWithSrid(points []*Point, srid int32, hasZ bool) (*MultiPoint, error) {
	if len(points) == 0 {
		return newEmptyMultiPointWithSrid(srid, hasZ), nil
	}
	for _, p := range points {
		if err := validateSameSrid(srid, p.srid, "multi-point"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, p.hasZ, "multi-point"); err != nil {
			return nil, err
		}
	}
	return &MultiPoint{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, points: points}, nil
}

func newMultiLineStringWithSrid(lineStrings []*LineString, srid int32, hasZ bool) (*MultiLineString, error) {
	if len(lineStrings) == 0 {
		return newEmptyMultiLineStringWithSrid(srid, hasZ), nil
	}
	for _, ls := range lineStrings {
		if err := validateSameSrid(srid, ls.srid, "multi-line string"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, ls.hasZ, "multi-line string"); err != nil {
			return nil, err
		}
	}
	return &MultiLineString{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, lineStrings: lineStrings}, nil
}

func newMultiPolygonWithSrid(polygons []*Polygon, srid int32, hasZ bool) (*MultiPolygon, error) {
	if len(polygons) == 0 {
		return newEmptyMultiPolygonWithSrid(srid, hasZ), nil
	}
	for _, poly := range polygons {
		if err := validateSameSrid(srid, poly.srid, "multi-polygon"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, poly.hasZ, "multi-polygon"); err != nil {
			return nil, err
		}
	}
	return &MultiPolygon{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, polygons: polygons}, nil
}

func newGeometryCollectionWithSrid(geometries []Geometry, srid int32, hasZ bool) (*GeometryCollection, error) {
	if len(geometries) == 0 {
		return newEmptyGeometryCollectionWithSrid(srid, hasZ), nil
	}
	for _, g := range geometries {
		if err := validateSameSrid(srid, g.Srid(), "geometry collection"); err != nil {
			return nil, err
		}
		if err := validateSameHasZ(hasZ, g.HasZ(), "geometry collection"); err != nil {
			return nil, err
		}
	}
	return &GeometryCollection{geometryBase: geometryBase{srid: srid, hasZ: hasZ}, geometries: geometries}, nil
}

// ------------------------------------------------------------------------
// Accessors
// ------------------------------------------------------------------------

// X returns the x coordinate of this point, a longitude under Model.Geographic. An empty point
// holds no coordinate, so for one the second result is false and the first is NaN.
func (p *Point) X() (float64, bool) {
	if p.empty {
		return math.NaN(), false
	}
	return p.x, true
}

// Y returns the y coordinate of this point, a latitude under Model.Geographic. An empty point
// holds no coordinate, so for one the second result is false and the first is NaN.
func (p *Point) Y() (float64, bool) {
	if p.empty {
		return math.NaN(), false
	}
	return p.y, true
}

// Z returns the z coordinate of this point. A 2D point and an empty point hold no z, so for them
// the second result is false and the first is NaN.
func (p *Point) Z() (float64, bool) {
	if p.empty || !p.hasZ {
		return math.NaN(), false
	}
	return p.z, true
}

// IsEmpty reports whether this point holds no coordinate.
func (p *Point) IsEmpty() bool {
	return p.empty
}

// Points returns the points of this line string, in order. The result is a copy, so changing it
// leaves the line string unchanged.
func (ls *LineString) Points() []*Point {
	return copyPoints(ls.points)
}

// IsEmpty reports whether this line string holds no points.
func (ls *LineString) IsEmpty() bool {
	return len(ls.points) == 0
}

// ExteriorRing returns the shell ring of this polygon, which is an empty line string when the
// polygon is empty.
func (poly *Polygon) ExteriorRing() *LineString {
	return poly.shellRing()
}

// InteriorRings returns the hole rings of this polygon, in order. The result is a copy, so
// changing it leaves the polygon unchanged.
func (poly *Polygon) InteriorRings() []*LineString {
	return copyLineStrings(poly.holes)
}

// IsEmpty reports whether the shell ring of this polygon holds no points.
func (poly *Polygon) IsEmpty() bool {
	return poly.shellRing().IsEmpty()
}

// shellRing returns the shell ring of this polygon. The zero value of Polygon carries no ring, so
// for it an empty ring of the same SRID and z dimension stands in and the zero value reads as an
// empty polygon.
func (poly *Polygon) shellRing() *LineString {
	if poly.shell == nil {
		return newEmptyLineStringWithSrid(poly.srid, poly.hasZ)
	}
	return poly.shell
}

// Points returns the points of this multi-point, in order. The result is a copy, so changing it
// leaves the multi-point unchanged.
func (mp *MultiPoint) Points() []*Point {
	return copyPoints(mp.points)
}

// IsEmpty reports whether this multi-point holds no parts, or only empty points.
func (mp *MultiPoint) IsEmpty() bool {
	for _, p := range mp.points {
		if !p.IsEmpty() {
			return false
		}
	}
	return true
}

// LineStrings returns the line strings of this multi-line string, in order. The result is a copy,
// so changing it leaves the multi-line string unchanged.
func (mls *MultiLineString) LineStrings() []*LineString {
	return copyLineStrings(mls.lineStrings)
}

// IsEmpty reports whether this multi-line string holds no parts, or only empty line strings.
func (mls *MultiLineString) IsEmpty() bool {
	for _, ls := range mls.lineStrings {
		if !ls.IsEmpty() {
			return false
		}
	}
	return true
}

// Polygons returns the polygons of this multi-polygon, in order. The result is a copy, so
// changing it leaves the multi-polygon unchanged.
func (mpoly *MultiPolygon) Polygons() []*Polygon {
	return copyPolygons(mpoly.polygons)
}

// IsEmpty reports whether this multi-polygon holds no parts, or only empty polygons.
func (mpoly *MultiPolygon) IsEmpty() bool {
	for _, poly := range mpoly.polygons {
		if !poly.IsEmpty() {
			return false
		}
	}
	return true
}

// Geometries returns the parts of this collection, in order. The result is a copy, so changing it
// leaves the collection unchanged.
func (gc *GeometryCollection) Geometries() []Geometry {
	return copyGeometries(gc.geometries)
}

// IsEmpty reports whether this collection holds no parts, or only empty parts.
func (gc *GeometryCollection) IsEmpty() bool {
	for _, g := range gc.geometries {
		if !g.IsEmpty() {
			return false
		}
	}
	return true
}

func (p *Point) hasNoParts() bool {
	return p.empty
}

func (ls *LineString) hasNoParts() bool {
	return len(ls.points) == 0
}

func (poly *Polygon) hasNoParts() bool {
	return poly.shellRing().hasNoParts()
}

func (mp *MultiPoint) hasNoParts() bool {
	return len(mp.points) == 0
}

func (mls *MultiLineString) hasNoParts() bool {
	return len(mls.lineStrings) == 0
}

func (mpoly *MultiPolygon) hasNoParts() bool {
	return len(mpoly.polygons) == 0
}

func (gc *GeometryCollection) hasNoParts() bool {
	return len(gc.geometries) == 0
}

// ------------------------------------------------------------------------
// Equality
// ------------------------------------------------------------------------

// Equal reports whether other is a point with the same SRID, the same z dimension and the same
// coordinates. Coordinates are compared numerically, so -0.0 equals 0.0. Two empty points are
// equal when their SRID and z dimension match, although their coordinates are NaN.
func (p *Point) Equal(other Geometry) bool {
	o, ok := other.(*Point)
	if !ok || o == nil {
		return false
	}
	if p.srid != o.srid || p.hasZ != o.hasZ || p.empty != o.empty {
		return false
	}
	if p.empty {
		return true
	}
	if p.x != o.x || p.y != o.y {
		return false
	}
	if p.hasZ {
		return p.z == o.z
	}
	return true
}

// Equal reports whether other is a line string with the same SRID, the same z dimension and the
// same points in the same order.
func (ls *LineString) Equal(other Geometry) bool {
	o, ok := other.(*LineString)
	if !ok || o == nil {
		return false
	}
	return ls.srid == o.srid && ls.hasZ == o.hasZ && equalPoints(ls.points, o.points)
}

// Equal reports whether other is a polygon with the same SRID, the same z dimension, the same
// shell ring and the same hole rings in the same order.
func (poly *Polygon) Equal(other Geometry) bool {
	o, ok := other.(*Polygon)
	if !ok || o == nil {
		return false
	}
	return poly.srid == o.srid && poly.hasZ == o.hasZ &&
		poly.shellRing().Equal(o.shellRing()) && equalLineStrings(poly.holes, o.holes)
}

// Equal reports whether other is a multi-point with the same SRID, the same z dimension and the
// same points in the same order.
func (mp *MultiPoint) Equal(other Geometry) bool {
	o, ok := other.(*MultiPoint)
	if !ok || o == nil {
		return false
	}
	return mp.srid == o.srid && mp.hasZ == o.hasZ && equalPoints(mp.points, o.points)
}

// Equal reports whether other is a multi-line string with the same SRID, the same z dimension
// and the same line strings in the same order.
func (mls *MultiLineString) Equal(other Geometry) bool {
	o, ok := other.(*MultiLineString)
	if !ok || o == nil {
		return false
	}
	return mls.srid == o.srid && mls.hasZ == o.hasZ && equalLineStrings(mls.lineStrings, o.lineStrings)
}

// Equal reports whether other is a multi-polygon with the same SRID, the same z dimension and
// the same polygons in the same order.
func (mpoly *MultiPolygon) Equal(other Geometry) bool {
	o, ok := other.(*MultiPolygon)
	if !ok || o == nil {
		return false
	}
	return mpoly.srid == o.srid && mpoly.hasZ == o.hasZ && equalPolygons(mpoly.polygons, o.polygons)
}

// Equal reports whether other is a geometry collection with the same SRID, the same z dimension
// and the same parts in the same order. A multi-point, multi-line string or multi-polygon is a
// kind of its own, so a collection never equals one.
func (gc *GeometryCollection) Equal(other Geometry) bool {
	o, ok := other.(*GeometryCollection)
	if !ok || o == nil {
		return false
	}
	return gc.srid == o.srid && gc.hasZ == o.hasZ && equalGeometries(gc.geometries, o.geometries)
}
