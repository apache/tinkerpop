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
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Only the geometry types of this package satisfy Geometry and its sub-interfaces, because the
// interfaces carry unexported methods.
var (
	_ Geometry     = (*Point)(nil)
	_ Geometry     = (*GeometryCollection)(nil)
	_ Curve        = (*LineString)(nil)
	_ Surface      = (*Polygon)(nil)
	_ Geometry     = (*MultiPoint)(nil)
	_ MultiCurve   = (*MultiLineString)(nil)
	_ MultiSurface = (*MultiPolygon)(nil)
)

func TestGeometryConstruction(t *testing.T) {
	t.Run("point holds its coordinates and reports the geographic model", func(t *testing.T) {
		p := seattlePoint(t)

		x, ok := p.X()
		assert.True(t, ok)
		assert.Equal(t, -122.3321, x)
		y, ok := p.Y()
		assert.True(t, ok)
		assert.Equal(t, 47.6062, y)
		assert.False(t, p.HasZ())
		assert.False(t, p.IsEmpty())
		assert.Equal(t, Model.Geographic, p.Model())
		assert.Equal(t, int32(4326), p.Srid())
	})

	t.Run("a point without z reports no z", func(t *testing.T) {
		z, ok := mustPoint(t, 1, 2, Model.Cartesian).Z()
		assert.False(t, ok)
		assert.True(t, math.IsNaN(z))
	})

	t.Run("point keeps a z coordinate", func(t *testing.T) {
		p := mustPointZ(t, 1, 2, 3, Model.Cartesian)

		assert.True(t, p.HasZ())
		z, ok := p.Z()
		assert.True(t, ok)
		assert.Equal(t, 3.0, z)
	})

	t.Run("empty point holds no coordinate", func(t *testing.T) {
		p := mustEmptyPoint(t, Model.Cartesian)

		assert.True(t, p.IsEmpty())
		x, ok := p.X()
		assert.False(t, ok)
		assert.True(t, math.IsNaN(x))
		y, ok := p.Y()
		assert.False(t, ok)
		assert.True(t, math.IsNaN(y))
	})

	t.Run("point rejects NaN and infinite coordinates", func(t *testing.T) {
		_, err := NewPoint(math.NaN(), 2, Model.Cartesian)
		assert.NotNil(t, err)
		_, err = NewPoint(math.Inf(1), 2, Model.Cartesian)
		assert.NotNil(t, err)
		_, err = NewPoint(1, math.Inf(-1), Model.Cartesian)
		assert.NotNil(t, err)
		_, err = NewPointZ(1, 2, math.NaN(), Model.Cartesian)
		assert.NotNil(t, err)
		_, err = NewPointZ(1, 2, math.Inf(1), Model.Cartesian)
		assert.NotNil(t, err)
	})

	t.Run("geographic point bounds its coordinates", func(t *testing.T) {
		_, err := NewPoint(180.5, 0, Model.Geographic)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "GEOGRAPHIC longitude must be in [-180, 180]")
		_, err = NewPoint(-180.5, 0, Model.Geographic)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "GEOGRAPHIC longitude must be in [-180, 180]")
		_, err = NewPoint(0, -90.5, Model.Geographic)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "GEOGRAPHIC latitude must be in [-90, 90]")
		_, err = NewPoint(0, 90.5, Model.Geographic)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "GEOGRAPHIC latitude must be in [-90, 90]")

		_, err = NewPoint(180, -90, Model.Geographic)
		assert.Nil(t, err)
		_, err = NewPoint(-180, 90, Model.Geographic)
		assert.Nil(t, err)
	})

	t.Run("cartesian point accepts coordinates of any size", func(t *testing.T) {
		p := mustPoint(t, 1e7, -1e7, Model.Cartesian)
		assert.Equal(t, Model.Cartesian, p.Model())
		assert.Equal(t, int32(0), p.Srid())
	})

	t.Run("a point with another srid reports the other model", func(t *testing.T) {
		p := mustPointWithSrid(t, 1, 2, 3857)

		assert.Equal(t, int32(3857), p.Srid())
		assert.Equal(t, Model.Other, p.Model())
	})

	t.Run("an empty geometry takes an explicit srid", func(t *testing.T) {
		for _, testCase := range []struct {
			name     string
			geometry Geometry
			kind     Geometry
		}{
			{"point", NewEmptyPointWithSrid(3857), &Point{}},
			{"line string", NewEmptyLineStringWithSrid(3857), &LineString{}},
			{"polygon", NewEmptyPolygonWithSrid(3857), &Polygon{}},
			{"multi-point", NewEmptyMultiPointWithSrid(3857), &MultiPoint{}},
			{"multi-line string", NewEmptyMultiLineStringWithSrid(3857), &MultiLineString{}},
			{"multi-polygon", NewEmptyMultiPolygonWithSrid(3857), &MultiPolygon{}},
			{"geometry collection", NewEmptyGeometryCollectionWithSrid(3857), &GeometryCollection{}},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				assert.IsType(t, testCase.kind, testCase.geometry)
				assert.True(t, testCase.geometry.IsEmpty())
				assert.False(t, testCase.geometry.HasZ())
				assert.Equal(t, int32(3857), testCase.geometry.Srid())
				assert.Equal(t, Model.Other, testCase.geometry.Model())
			})
		}
	})

	t.Run("constructors reject the other model", func(t *testing.T) {
		_, err := NewPoint(1, 2, Model.Other)
		assert.NotNil(t, err)
		_, err = NewPointZ(1, 2, 3, Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyPoint(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyLineString(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyPolygon(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyMultiPoint(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyMultiLineString(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyMultiPolygon(Model.Other)
		assert.NotNil(t, err)
		_, err = NewEmptyGeometryCollection(Model.Other)
		assert.NotNil(t, err)
	})

	t.Run("constructors reject an unknown model", func(t *testing.T) {
		_, err := NewPoint(1, 2, model("MERCATOR"))
		assert.NotNil(t, err)
		_, err = NewPoint(1, 2, model(""))
		assert.NotNil(t, err)
	})

	t.Run("line string takes 0 or at least 2 points", func(t *testing.T) {
		_, err := NewLineString()
		assert.NotNil(t, err)
		_, err = NewLineString(mustPoint(t, 1, 2, Model.Cartesian))
		assert.NotNil(t, err)

		empty, err := NewEmptyLineString(Model.Cartesian)
		assert.Nil(t, err)
		assert.True(t, empty.IsEmpty())
		assert.Equal(t, 0, len(empty.Points()))
	})

	t.Run("line string rejects an empty point as a vertex", func(t *testing.T) {
		_, err := NewLineString(mustEmptyPoint(t, Model.Cartesian), mustPoint(t, 1, 1, Model.Cartesian))
		assert.NotNil(t, err)
	})

	t.Run("line string rejects mixed z dimensions", func(t *testing.T) {
		_, err := NewLineString(
			mustPoint(t, 0, 0, Model.Cartesian),
			mustPointZ(t, 1, 1, 1, Model.Cartesian))
		assert.NotNil(t, err)
	})

	t.Run("polygon rings must be closed and hold at least 4 points", func(t *testing.T) {
		open := mustLineString(t,
			mustPoint(t, 0, 0, Model.Cartesian),
			mustPoint(t, 1, 0, Model.Cartesian),
			mustPoint(t, 1, 1, Model.Cartesian),
			mustPoint(t, 0, 1, Model.Cartesian))
		_, err := NewPolygon(open)
		assert.NotNil(t, err)

		short := mustLineString(t,
			mustPoint(t, 0, 0, Model.Cartesian),
			mustPoint(t, 1, 1, Model.Cartesian),
			mustPoint(t, 0, 0, Model.Cartesian))
		_, err = NewPolygon(short)
		assert.NotNil(t, err)
	})

	t.Run("polygon rejects an empty hole or an empty shell with holes", func(t *testing.T) {
		shell := geographicPolygon(t).ExteriorRing()
		emptyRing, err := NewEmptyLineString(Model.Geographic)
		assert.Nil(t, err)

		_, err = NewPolygon(shell, emptyRing)
		assert.NotNil(t, err)

		hole := geographicPolygon(t).InteriorRings()[0]
		_, err = NewPolygon(emptyRing, hole)
		assert.NotNil(t, err)
	})

	t.Run("multi kinds reject parts of different srids", func(t *testing.T) {
		_, err := NewMultiPoint(
			mustPoint(t, 1, 2, Model.Cartesian),
			mustPointWithSrid(t, 3, 4, 3857))
		assert.NotNil(t, err)

		_, err = NewMultiLineString(
			cartesianLineString(t),
			mustLineString(t,
				mustPointWithSrid(t, 0, 0, 3857),
				mustPointWithSrid(t, 1, 1, 3857)))
		assert.NotNil(t, err)

		_, err = NewMultiPolygon(
			mustPolygon(t, mustLineString(t,
				mustPoint(t, 0, 0, Model.Cartesian),
				mustPoint(t, 1, 0, Model.Cartesian),
				mustPoint(t, 1, 1, Model.Cartesian),
				mustPoint(t, 0, 0, Model.Cartesian))),
			mustPolygon(t, mustLineString(t,
				mustPointWithSrid(t, 0, 0, 3857),
				mustPointWithSrid(t, 1, 0, 3857),
				mustPointWithSrid(t, 1, 1, 3857),
				mustPointWithSrid(t, 0, 0, 3857))))
		assert.NotNil(t, err)

		_, err = NewGeometryCollection(
			mustPoint(t, 1, 2, Model.Cartesian),
			mustPoint(t, 3, 4, Model.Geographic))
		assert.NotNil(t, err)
	})

	t.Run("multi kinds need at least one part", func(t *testing.T) {
		_, err := NewMultiPoint()
		assert.NotNil(t, err)
		_, err = NewMultiLineString()
		assert.NotNil(t, err)
		_, err = NewMultiPolygon()
		assert.NotNil(t, err)
		_, err = NewGeometryCollection()
		assert.NotNil(t, err)
	})

	t.Run("a nil part is rejected", func(t *testing.T) {
		_, err := NewLineString(mustPoint(t, 0, 0, Model.Cartesian), nil)
		assert.NotNil(t, err)
		_, err = NewMultiPoint(nil)
		assert.NotNil(t, err)
		_, err = NewPolygon(nil)
		assert.NotNil(t, err)
		_, err = NewGeometryCollection(nil)
		assert.NotNil(t, err)
	})

	t.Run("a nil pointer of a geometry type is rejected as a part", func(t *testing.T) {
		_, err := NewGeometryCollection((*Point)(nil))
		assert.NotNil(t, err)
		_, err = NewGeometryCollection((*GeometryCollection)(nil))
		assert.NotNil(t, err)
		_, err = NewGeometryCollection(mustPoint(t, 1, 2, Model.Cartesian), (*LineString)(nil))
		assert.NotNil(t, err)
	})

	t.Run("the zero value of a polygon reads as an empty polygon", func(t *testing.T) {
		zero := &Polygon{}

		assert.True(t, zero.IsEmpty())
		assert.Equal(t, "POLYGON EMPTY", zero.String())
		assert.Equal(t, "000000000300000000", hex.EncodeToString(zero.WKB()))
		assert.True(t, zero.ExteriorRing().IsEmpty())
		assert.Equal(t, 0, len(zero.InteriorRings()))

		empty, err := NewEmptyPolygon(Model.Cartesian)
		assert.Nil(t, err)
		assert.True(t, zero.Equal(empty))
	})

	t.Run("part accessors hand out a copy", func(t *testing.T) {
		mp := cartesianMultiPoint(t)

		points := mp.Points()
		assert.Equal(t, 2, len(points))
		assert.True(t, points[0].Equal(mustPoint(t, 1, 2, Model.Cartesian)))
		points[0] = mustPoint(t, 9, 9, Model.Cartesian)
		assert.True(t, mp.Points()[0].Equal(mustPoint(t, 1, 2, Model.Cartesian)))

		gc := nestedCollection(t)
		parts := gc.Geometries()
		parts[0] = mustPoint(t, 9, 9, Model.Cartesian)
		assert.True(t, gc.Geometries()[0].Equal(mustPoint(t, 1, 2, Model.Cartesian)))

		ls := cartesianLineString(t)
		vertices := ls.Points()
		vertices[1] = mustPoint(t, 9, 9, Model.Cartesian)
		assert.True(t, ls.Points()[1].Equal(mustPoint(t, 1, 1, Model.Cartesian)))

		poly := geographicPolygon(t)
		holes := poly.InteriorRings()
		assert.Equal(t, 1, len(holes))
		holes[0] = postgisPolygon(t).ExteriorRing()
		assert.True(t, poly.InteriorRings()[0].Equal(geographicPolygon(t).InteriorRings()[0]))

		mls := cartesianMultiLineString(t)
		strands := mls.LineStrings()
		assert.Equal(t, 2, len(strands))
		strands[0] = cartesianLineString(t)
		assert.True(t, mls.LineStrings()[0].Equal(cartesianMultiLineString(t).LineStrings()[0]))

		mpoly := geographicMultiPolygon(t)
		polygons := mpoly.Polygons()
		assert.Equal(t, 2, len(polygons))
		polygons[0] = postgisPolygon(t)
		assert.True(t, mpoly.Polygons()[0].Equal(geographicMultiPolygon(t).Polygons()[0]))
	})

	t.Run("collection nesting is bounded", func(t *testing.T) {
		var deepest Geometry = mustPoint(t, 1, 2, Model.Cartesian)
		for level := 0; level < 32; level++ {
			gc, err := NewGeometryCollection(deepest)
			assert.Nil(t, err)
			deepest = gc
		}

		_, err := NewGeometryCollection(deepest)
		assert.NotNil(t, err)
	})

	t.Run("a multi-point counts as one level of nesting", func(t *testing.T) {
		var deepest Geometry = cartesianMultiPoint(t)
		for level := 0; level < 31; level++ {
			gc, err := NewGeometryCollection(deepest)
			assert.Nil(t, err)
			deepest = gc
		}

		_, err := NewGeometryCollection(deepest)
		assert.NotNil(t, err)
	})

	t.Run("a collection is empty when all of its parts are", func(t *testing.T) {
		assert.True(t, mustMultiPoint(t, mustEmptyPoint(t, Model.Cartesian)).IsEmpty())
		assert.False(t, cartesianMultiPoint(t).IsEmpty())
		assert.False(t, nestedCollection(t).IsEmpty())
	})
}

func TestGeometryEquality(t *testing.T) {
	t.Run("geometries with the same values are equal", func(t *testing.T) {
		assert.True(t, seattlePoint(t).Equal(seattlePoint(t)))
		assert.True(t, mustPointZ(t, 1, 2, 3, Model.Cartesian).Equal(mustPointZ(t, 1, 2, 3, Model.Cartesian)))
		assert.True(t, cartesianLineString(t).Equal(cartesianLineString(t)))
		assert.True(t, geographicPolygon(t).Equal(geographicPolygon(t)))
		assert.True(t, cartesianMultiPoint(t).Equal(cartesianMultiPoint(t)))
		assert.True(t, cartesianMultiLineString(t).Equal(cartesianMultiLineString(t)))
		assert.True(t, geographicMultiPolygon(t).Equal(geographicMultiPolygon(t)))
		assert.True(t, nestedCollection(t).Equal(nestedCollection(t)))
	})

	t.Run("negative zero equals zero", func(t *testing.T) {
		negativeZero := math.Copysign(0, -1)
		assert.True(t, mustPoint(t, negativeZero, 0, Model.Cartesian).Equal(mustPoint(t, 0, 0, Model.Cartesian)))
		assert.True(t, mustPointZ(t, 0, 0, negativeZero, Model.Cartesian).Equal(
			mustPointZ(t, 0, 0, 0, Model.Cartesian)))
	})

	t.Run("two empty points of the same srid are equal", func(t *testing.T) {
		assert.True(t, mustEmptyPoint(t, Model.Cartesian).Equal(mustEmptyPoint(t, Model.Cartesian)))
		assert.False(t, mustEmptyPoint(t, Model.Cartesian).Equal(mustEmptyPoint(t, Model.Geographic)))
		assert.False(t, mustEmptyPoint(t, Model.Cartesian).Equal(mustPoint(t, 0, 0, Model.Cartesian)))
	})

	t.Run("a different srid, z dimension or kind is not equal", func(t *testing.T) {
		assert.False(t, mustPoint(t, 1, 2, Model.Cartesian).Equal(mustPointWithSrid(t, 1, 2, 3857)))
		assert.False(t, mustPoint(t, 1, 2, Model.Cartesian).Equal(mustPointZ(t, 1, 2, 0, Model.Cartesian)))
		assert.False(t, cartesianMultiPoint(t).Equal(mustGeometryCollection(t,
			mustPoint(t, 1, 2, Model.Cartesian),
			mustPoint(t, 3, 4, Model.Cartesian))))
		assert.False(t, seattlePoint(t).Equal(nil))
	})

	t.Run("parts are compared in order", func(t *testing.T) {
		reversed := mustMultiPoint(t,
			mustPoint(t, 3, 4, Model.Cartesian),
			mustPoint(t, 1, 2, Model.Cartesian))
		assert.False(t, cartesianMultiPoint(t).Equal(reversed))
	})

	t.Run("a different coordinate is not equal", func(t *testing.T) {
		assert.False(t, mustPoint(t, 1, 2, Model.Cartesian).Equal(mustPoint(t, 9, 2, Model.Cartesian)))
		assert.False(t, mustPoint(t, 1, 2, Model.Cartesian).Equal(mustPoint(t, 1, 9, Model.Cartesian)))
		assert.False(t, mustPointZ(t, 1, 2, 3, Model.Cartesian).Equal(
			mustPointZ(t, 1, 2, 4, Model.Cartesian)))
	})

	t.Run("a different number of parts is not equal", func(t *testing.T) {
		// The shorter line string is a prefix of the longer one, so only the count differs.
		twoVertices := mustLineString(t,
			mustPoint(t, 0, 0, Model.Cartesian),
			mustPoint(t, 1, 1, Model.Cartesian))
		assert.False(t, twoVertices.Equal(cartesianLineString(t)))
		assert.False(t, cartesianLineString(t).Equal(twoVertices))

		onePoint := mustMultiPoint(t, mustPoint(t, 1, 2, Model.Cartesian))
		assert.False(t, onePoint.Equal(cartesianMultiPoint(t)))
		assert.False(t, cartesianMultiPoint(t).Equal(onePoint))
	})

	t.Run("polygons with different hole rings are not equal", func(t *testing.T) {
		withHole := geographicPolygon(t)
		shellOnly := mustPolygon(t, withHole.ExteriorRing())
		assert.False(t, withHole.Equal(shellOnly))
		assert.False(t, shellOnly.Equal(withHole))

		otherHole := mustPolygon(t, withHole.ExteriorRing(), mustLineString(t,
			mustPoint(t, 6, 6, Model.Geographic),
			mustPoint(t, 6, 8, Model.Geographic),
			mustPoint(t, 8, 8, Model.Geographic),
			mustPoint(t, 6, 6, Model.Geographic)))
		assert.False(t, withHole.Equal(otherHole))
	})

	t.Run("a geometry with no parts and a z dimension is not equal to a 2D one", func(t *testing.T) {
		// LINESTRING Z EMPTY and MULTIPOINT Z EMPTY, which only WKB can express.
		lineStringZ := mustGeometryFromWkbHex(t, "00"+"000003ea"+"00000000", 0)
		lineString, err := NewEmptyLineString(Model.Cartesian)
		assert.Nil(t, err)
		assert.True(t, lineStringZ.HasZ())
		assert.False(t, lineString.Equal(lineStringZ))
		assert.False(t, lineStringZ.Equal(lineString))

		multiPointZ := mustGeometryFromWkbHex(t, "00"+"000003ec"+"00000000", 0)
		multiPoint, err := NewEmptyMultiPoint(Model.Cartesian)
		assert.Nil(t, err)
		assert.True(t, multiPointZ.HasZ())
		assert.False(t, multiPoint.Equal(multiPointZ))
		assert.False(t, multiPointZ.Equal(multiPoint))
	})

	t.Run("a nil pointer of a geometry type is not equal", func(t *testing.T) {
		assert.False(t, seattlePoint(t).Equal((*Point)(nil)))
		assert.False(t, cartesianLineString(t).Equal((*LineString)(nil)))
		assert.False(t, geographicPolygon(t).Equal((*Polygon)(nil)))
		assert.False(t, cartesianMultiPoint(t).Equal((*MultiPoint)(nil)))
		assert.False(t, cartesianMultiLineString(t).Equal((*MultiLineString)(nil)))
		assert.False(t, geographicMultiPolygon(t).Equal((*MultiPolygon)(nil)))
		assert.False(t, nestedCollection(t).Equal((*GeometryCollection)(nil)))
	})
}

func mustPoint(t *testing.T, x, y float64, m model) *Point {
	t.Helper()
	p, err := NewPoint(x, y, m)
	assert.Nil(t, err)
	return p
}

func mustPointZ(t *testing.T, x, y, z float64, m model) *Point {
	t.Helper()
	p, err := NewPointZ(x, y, z, m)
	assert.Nil(t, err)
	return p
}

func mustPointWithSrid(t *testing.T, x, y float64, srid int32) *Point {
	t.Helper()
	p, err := NewPointWithSrid(x, y, srid)
	assert.Nil(t, err)
	return p
}

func mustEmptyPoint(t *testing.T, m model) *Point {
	t.Helper()
	p, err := NewEmptyPoint(m)
	assert.Nil(t, err)
	return p
}

func mustLineString(t *testing.T, points ...*Point) *LineString {
	t.Helper()
	ls, err := NewLineString(points...)
	assert.Nil(t, err)
	return ls
}

func mustPolygon(t *testing.T, shell *LineString, holes ...*LineString) *Polygon {
	t.Helper()
	poly, err := NewPolygon(shell, holes...)
	assert.Nil(t, err)
	return poly
}

func mustMultiPoint(t *testing.T, points ...*Point) *MultiPoint {
	t.Helper()
	mp, err := NewMultiPoint(points...)
	assert.Nil(t, err)
	return mp
}

func mustMultiLineString(t *testing.T, lineStrings ...*LineString) *MultiLineString {
	t.Helper()
	mls, err := NewMultiLineString(lineStrings...)
	assert.Nil(t, err)
	return mls
}

func mustMultiPolygon(t *testing.T, polygons ...*Polygon) *MultiPolygon {
	t.Helper()
	mpoly, err := NewMultiPolygon(polygons...)
	assert.Nil(t, err)
	return mpoly
}

func mustGeometryCollection(t *testing.T, geometries ...Geometry) *GeometryCollection {
	t.Helper()
	gc, err := NewGeometryCollection(geometries...)
	assert.Nil(t, err)
	return gc
}

// mustGeometryFromWkbHex reads a geometry from ISO WKB given as a hex string, which reaches the
// shapes the constructors do not build, such as a 3D geometry with no parts.
func mustGeometryFromWkbHex(t *testing.T, hexBytes string, srid int32) Geometry {
	t.Helper()
	wkb, err := hex.DecodeString(hexBytes)
	assert.Nil(t, err)
	g, err := GeometryFromWKB(wkb, srid)
	assert.Nil(t, err)
	return g
}

// seattlePoint returns POINT(-122.3321 47.6062) under the geographic model, the geometry the
// GraphBinary wire layout is documented with.
func seattlePoint(t *testing.T) *Point {
	t.Helper()
	return mustPoint(t, -122.3321, 47.6062, Model.Geographic)
}

// cartesianLineString returns LINESTRING(0 0, 1 1, 2 0).
func cartesianLineString(t *testing.T) *LineString {
	t.Helper()
	return mustLineString(t,
		mustPoint(t, 0, 0, Model.Cartesian),
		mustPoint(t, 1, 1, Model.Cartesian),
		mustPoint(t, 2, 0, Model.Cartesian))
}

// geographicPolygon returns POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2)).
func geographicPolygon(t *testing.T) *Polygon {
	t.Helper()
	shell := mustLineString(t,
		mustPoint(t, 0, 0, Model.Geographic),
		mustPoint(t, 10, 0, Model.Geographic),
		mustPoint(t, 10, 10, Model.Geographic),
		mustPoint(t, 0, 10, Model.Geographic),
		mustPoint(t, 0, 0, Model.Geographic))
	hole := mustLineString(t,
		mustPoint(t, 2, 2, Model.Geographic),
		mustPoint(t, 2, 4, Model.Geographic),
		mustPoint(t, 4, 4, Model.Geographic),
		mustPoint(t, 2, 2, Model.Geographic))
	return mustPolygon(t, shell, hole)
}

// postgisPolygon returns POLYGON((10 20, 30 20, 30 40, 10 40, 10 20)) under the geographic model.
func postgisPolygon(t *testing.T) *Polygon {
	t.Helper()
	return mustPolygon(t, mustLineString(t,
		mustPoint(t, 10, 20, Model.Geographic),
		mustPoint(t, 30, 20, Model.Geographic),
		mustPoint(t, 30, 40, Model.Geographic),
		mustPoint(t, 10, 40, Model.Geographic),
		mustPoint(t, 10, 20, Model.Geographic)))
}

// cartesianMultiPoint returns MULTIPOINT((1 2), (3 4)).
func cartesianMultiPoint(t *testing.T) *MultiPoint {
	t.Helper()
	return mustMultiPoint(t,
		mustPoint(t, 1, 2, Model.Cartesian),
		mustPoint(t, 3, 4, Model.Cartesian))
}

// cartesianMultiLineString returns MULTILINESTRING((0 0, 1 1), (2 2, 3 3)).
func cartesianMultiLineString(t *testing.T) *MultiLineString {
	t.Helper()
	first := mustLineString(t,
		mustPoint(t, 0, 0, Model.Cartesian),
		mustPoint(t, 1, 1, Model.Cartesian))
	second := mustLineString(t,
		mustPoint(t, 2, 2, Model.Cartesian),
		mustPoint(t, 3, 3, Model.Cartesian))
	return mustMultiLineString(t, first, second)
}

// geographicMultiPolygon returns
// MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2))).
func geographicMultiPolygon(t *testing.T) *MultiPolygon {
	t.Helper()
	first := mustPolygon(t, mustLineString(t,
		mustPoint(t, 0, 0, Model.Geographic),
		mustPoint(t, 1, 0, Model.Geographic),
		mustPoint(t, 1, 1, Model.Geographic),
		mustPoint(t, 0, 1, Model.Geographic),
		mustPoint(t, 0, 0, Model.Geographic)))
	second := mustPolygon(t, mustLineString(t,
		mustPoint(t, 2, 2, Model.Geographic),
		mustPoint(t, 3, 2, Model.Geographic),
		mustPoint(t, 3, 3, Model.Geographic),
		mustPoint(t, 2, 3, Model.Geographic),
		mustPoint(t, 2, 2, Model.Geographic)))
	return mustMultiPolygon(t, first, second)
}

// nestedCollection returns
// GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY).
func nestedCollection(t *testing.T) *GeometryCollection {
	t.Helper()
	inner := mustGeometryCollection(t, mustLineString(t,
		mustPoint(t, 0, 0, Model.Cartesian),
		mustPoint(t, 1, 1, Model.Cartesian)))
	return mustGeometryCollection(t,
		mustPoint(t, 1, 2, Model.Cartesian),
		inner,
		mustEmptyPoint(t, Model.Cartesian))
}
