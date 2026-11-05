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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGeometryWkt(t *testing.T) {
	t.Run("each kind writes the ISO WKT text", func(t *testing.T) {
		assert.Equal(t, "POINT(-122.3321 47.6062)", seattlePoint(t).String())
		assert.Equal(t, "POINT Z (1 2 3)", mustPointZ(t, 1, 2, 3, Model.Cartesian).String())
		assert.Equal(t, "POINT EMPTY", mustEmptyPoint(t, Model.Cartesian).String())
		assert.Equal(t, "LINESTRING(0 0, 1 1, 2 0)", cartesianLineString(t).String())
		assert.Equal(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))",
			geographicPolygon(t).String())
		assert.Equal(t, "MULTIPOINT((1 2), (3 4))", cartesianMultiPoint(t).String())
		assert.Equal(t, "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))", cartesianMultiLineString(t).String())
		assert.Equal(t, "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
			geographicMultiPolygon(t).String())
		assert.Equal(t, "GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)",
			nestedCollection(t).String())
	})

	t.Run("WKT writes the same text as String", func(t *testing.T) {
		for _, geometry := range []Geometry{
			seattlePoint(t),
			mustPointZ(t, 1, 2, 3, Model.Cartesian),
			mustEmptyPoint(t, Model.Cartesian),
			cartesianLineString(t),
			geographicPolygon(t),
			cartesianMultiPoint(t),
			cartesianMultiLineString(t),
			geographicMultiPolygon(t),
			nestedCollection(t),
		} {
			assert.Equal(t, geometry.String(), geometry.WKT())
		}
	})

	t.Run("a geometry with no parts writes the EMPTY keyword", func(t *testing.T) {
		emptyLineString, err := NewEmptyLineString(Model.Cartesian)
		assert.Nil(t, err)
		assert.Equal(t, "LINESTRING EMPTY", emptyLineString.String())

		emptyPolygon, err := NewEmptyPolygon(Model.Cartesian)
		assert.Nil(t, err)
		assert.Equal(t, "POLYGON EMPTY", emptyPolygon.String())

		emptyCollection, err := NewEmptyGeometryCollection(Model.Cartesian)
		assert.Nil(t, err)
		assert.Equal(t, "GEOMETRYCOLLECTION EMPTY", emptyCollection.String())
	})

	t.Run("an empty part of a multi kind writes the EMPTY keyword", func(t *testing.T) {
		assert.Equal(t, "MULTIPOINT((1 2), EMPTY)", mustMultiPoint(t,
			mustPoint(t, 1, 2, Model.Cartesian),
			mustEmptyPoint(t, Model.Cartesian)).String())
	})

	t.Run("EWKT carries an SRID prefix", func(t *testing.T) {
		assert.Equal(t, "SRID=4326;POINT(-122.3321 47.6062)", seattlePoint(t).EWKT())
		assert.Equal(t, "SRID=0;POINT(1 2)", mustPoint(t, 1, 2, Model.Cartesian).EWKT())
		assert.Equal(t, "SRID=0;POINT Z (1 2 3)", mustPointZ(t, 1, 2, 3, Model.Cartesian).EWKT())
		assert.Equal(t, "SRID=3857;POINT(1 2)", mustPointWithSrid(t, 1, 2, 3857).EWKT())
		assert.Equal(t, "SRID=0;POINT EMPTY", mustEmptyPoint(t, Model.Cartesian).EWKT())
		assert.Equal(t, "SRID=0;LINESTRING(0 0, 1 1, 2 0)", cartesianLineString(t).EWKT())
		assert.Equal(t, "SRID=4326;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))",
			geographicPolygon(t).EWKT())
		assert.Equal(t, "SRID=0;MULTIPOINT((1 2), (3 4))", cartesianMultiPoint(t).EWKT())
		assert.Equal(t, "SRID=0;MULTILINESTRING((0 0, 1 1), (2 2, 3 3))", cartesianMultiLineString(t).EWKT())
		assert.Equal(t, "SRID=4326;MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
			geographicMultiPolygon(t).EWKT())
		assert.Equal(t,
			"SRID=0;GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)",
			nestedCollection(t).EWKT())
	})

	t.Run("coordinates are written as the shortest plain decimal", func(t *testing.T) {
		assert.Equal(t, "0.1", formatWktCoordinate(0.1))
		assert.Equal(t, "1000000000000000", formatWktCoordinate(1e15))
		assert.Equal(t, "10000000000000000", formatWktCoordinate(1e16))
		assert.Equal(t, "1000000000000000000000", formatWktCoordinate(1e21))
		assert.Equal(t, "0.00001", formatWktCoordinate(1e-5))
		assert.Equal(t, "0.000001", formatWktCoordinate(1e-6))
		assert.Equal(t, "0.0000001", formatWktCoordinate(1e-7))
		assert.Equal(t, "-0", formatWktCoordinate(math.Copysign(0, -1)))
		assert.Equal(t, "0", formatWktCoordinate(0))
		assert.Equal(t, "1", formatWktCoordinate(1))
	})
}
