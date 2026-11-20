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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGeometryWkb(t *testing.T) {
	t.Run("each kind reads back from its own bytes", func(t *testing.T) {
		for _, original := range []Geometry{
			seattlePoint(t),
			mustPointZ(t, 1, 2, 3, Model.Cartesian),
			mustEmptyPoint(t, Model.Cartesian),
			cartesianLineString(t),
			geographicPolygon(t),
			cartesianMultiPoint(t),
			cartesianMultiLineString(t),
			geographicMultiPolygon(t),
			nestedCollection(t),
			mustGeometryCollection(t, mustPointZ(t, 1, 2, 3, Model.Cartesian)),
			mustPointWithSrid(t, 1, 2, 3857),
		} {
			read, err := GeometryFromWKB(original.WKB(), original.Srid())
			assert.Nil(t, err)
			assert.IsType(t, original, read)
			assert.True(t, original.Equal(read), "%s did not read back equal", original)
		}
	})

	t.Run("a little-endian geometry reads the same as a big-endian one", func(t *testing.T) {
		littleEndian, err := hex.DecodeString("0101000000" + "1ac05b2041955ec0" + "e86a2bf697cd4740")
		assert.Nil(t, err)

		read, err := GeometryFromWKB(littleEndian, 4326)
		assert.Nil(t, err)
		assert.True(t, seattlePoint(t).Equal(read))
	})

	t.Run("each nested geometry carries its own byte order", func(t *testing.T) {
		expected := cartesianMultiPoint(t)

		// A multi-point of (1 2) and (3 4) whose header and parts disagree on byte order.
		for _, hexBytes := range []string{
			"01" + "04000000" + "02000000" +
				"00" + "00000001" + "3ff0000000000000" + "4000000000000000" +
				"00" + "00000001" + "4008000000000000" + "4010000000000000",
			"00" + "00000004" + "00000002" +
				"01" + "01000000" + "000000000000f03f" + "0000000000000040" +
				"01" + "01000000" + "0000000000000840" + "0000000000001040",
		} {
			wkb, err := hex.DecodeString(hexBytes)
			assert.Nil(t, err)

			read, err := GeometryFromWKB(wkb, 0)
			assert.Nil(t, err)
			assert.True(t, expected.Equal(read), "%s did not read back equal", expected)
		}
	})

	t.Run("a 3D point with NaN coordinates reads as an empty point with a z tag", func(t *testing.T) {
		emptyPointZ := "00" + "000003e9" +
			"7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000"
		wkb, err := hex.DecodeString(emptyPointZ)
		assert.Nil(t, err)

		read, err := GeometryFromWKB(wkb, 0)
		assert.Nil(t, err)
		point, ok := read.(*Point)
		assert.True(t, ok)
		assert.True(t, point.IsEmpty())
		assert.True(t, point.HasZ())
		assert.Equal(t, "POINT Z EMPTY", point.String())
		assert.Equal(t, emptyPointZ, hex.EncodeToString(point.WKB()))
	})

	t.Run("a 3D geometry with no parts keeps its z tag", func(t *testing.T) {
		emptyLineStringZ := "00" + "000003ea" + "00000000"
		wkb, err := hex.DecodeString(emptyLineStringZ)
		assert.Nil(t, err)
		assert.Equal(t, 9, len(wkb))

		read, err := GeometryFromWKB(wkb, 0)
		assert.Nil(t, err)
		lineString, ok := read.(*LineString)
		assert.True(t, ok)
		assert.True(t, lineString.IsEmpty())
		assert.True(t, lineString.HasZ())
		assert.Equal(t, "LINESTRING Z EMPTY", lineString.String())
		assert.Equal(t, emptyLineStringZ, hex.EncodeToString(lineString.WKB()))

		emptyPolygonZ := "00" + "000003eb" + "00000000"
		wkb, err = hex.DecodeString(emptyPolygonZ)
		assert.Nil(t, err)
		assert.Equal(t, 9, len(wkb))

		read, err = GeometryFromWKB(wkb, 0)
		assert.Nil(t, err)
		polygon, ok := read.(*Polygon)
		assert.True(t, ok)
		assert.True(t, polygon.IsEmpty())
		assert.True(t, polygon.HasZ())
		assert.True(t, polygon.ExteriorRing().HasZ())
		assert.Equal(t, "POLYGON Z EMPTY", polygon.String())
		assert.Equal(t, emptyPolygonZ, hex.EncodeToString(polygon.WKB()))
	})

	t.Run("an invalid byte order marker is rejected", func(t *testing.T) {
		// A point whose byte order marker is neither 0x00 nor 0x01.
		wkb, err := hex.DecodeString("02" + "00000001" +
			"3ff0000000000000" + "4000000000000000")
		assert.Nil(t, err)
		assert.Equal(t, 21, len(wkb))

		_, err = GeometryFromWKB(wkb, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "invalid WKB byte order")
	})

	t.Run("a point with one NaN coordinate is rejected", func(t *testing.T) {
		// Only a wholly NaN coordinate encodes an empty point.
		nanX, err := hex.DecodeString("00" + "00000001" +
			"7ff8000000000000" + "4000000000000000")
		assert.Nil(t, err)
		assert.Equal(t, 21, len(nanX))

		_, err = GeometryFromWKB(nanX, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "x coordinate cannot be NaN or infinite")

		nanY, err := hex.DecodeString("00" + "00000001" +
			"3ff0000000000000" + "7ff8000000000000")
		assert.Nil(t, err)
		assert.Equal(t, 21, len(nanY))

		_, err = GeometryFromWKB(nanY, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "y coordinate cannot be NaN or infinite")
	})

	t.Run("a multi kind rejects a part of another kind", func(t *testing.T) {
		for _, testCase := range []struct {
			name    string
			hexWkb  string
			length  int
			message string
		}{
			{
				"a line string in a multi-point",
				"00" + "00000004" + "00000001" +
					"00" + "00000002" + "00000002" +
					"0000000000000000" + "0000000000000000" +
					"3ff0000000000000" + "3ff0000000000000",
				50,
				"expected a point in a multi-point, got LINESTRING",
			},
			{
				"a point in a multi-line string",
				"00" + "00000005" + "00000001" +
					"00" + "00000001" +
					"3ff0000000000000" + "4000000000000000",
				30,
				"expected a line string in a multi-line string, got POINT",
			},
			{
				"a point in a multi-polygon",
				"00" + "00000006" + "00000001" +
					"00" + "00000001" +
					"3ff0000000000000" + "4000000000000000",
				30,
				"expected a polygon in a multi-polygon, got POINT",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				wkb, err := hex.DecodeString(testCase.hexWkb)
				assert.Nil(t, err)
				assert.Equal(t, testCase.length, len(wkb))

				_, err = GeometryFromWKB(wkb, 0)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), testCase.message)
			})
		}
	})

	t.Run("unsupported type codes and counts are rejected", func(t *testing.T) {
		for _, testCase := range []struct {
			name    string
			hexWkb  string
			length  int
			message string
		}{
			{
				"the M dimension",
				"00" + "000007d1" +
					"3ff0000000000000" + "4000000000000000" + "4008000000000000",
				29,
				"M dimension not supported (type code 2001)",
			},
			{
				"the ZM dimension",
				"00" + "00000bb9" +
					"3ff0000000000000" + "4000000000000000" +
					"4008000000000000" + "4010000000000000",
				37,
				"ZM dimension not supported (type code 3001)",
			},
			{
				// The coordinates of a point follow, so parsing this as a point would succeed.
				"an unknown type code",
				"00" + "00000008" +
					"3ff0000000000000" + "4000000000000000",
				21,
				"unknown WKB type code: 8",
			},
			{
				// A line string whose point count does not fit a signed 32-bit integer.
				"a count above the signed 32-bit range",
				"00" + "00000002" + "80000000",
				9,
				"invalid point count: 2147483648",
			},
			{
				// A polygon of one ring that declares no points.
				"a ring with no points",
				"00" + "00000003" + "00000001" + "00000000",
				13,
				"a polygon ring must have at least 4 points, got 0",
			},
			{
				// A polygon of one closed ring that declares and carries three points, so the
				// ring is rejected on its point count rather than on its closure.
				"a ring with three points",
				"00" + "00000003" + "00000001" + "00000003" +
					"0000000000000000" + "0000000000000000" +
					"3ff0000000000000" + "0000000000000000" +
					"0000000000000000" + "0000000000000000",
				61,
				"a polygon ring must have at least 4 points, got 3",
			},
			{
				// The coordinates of a point follow, so this is rejected on its M flag alone.
				"an EWKB type code carrying only the M flag",
				"00" + "40000001" +
					"3ff0000000000000" + "4000000000000000",
				21,
				"EWKB format detected (type code 0x40000001); ISO WKB is expected." +
					" EWKB with Z, M, or SRID flags is not supported",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				wkb, err := hex.DecodeString(testCase.hexWkb)
				assert.Nil(t, err)
				assert.Equal(t, testCase.length, len(wkb))

				_, err = GeometryFromWKB(wkb, 0)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), testCase.message)
			})
		}
	})

	t.Run("empty input is rejected", func(t *testing.T) {
		_, err := GeometryFromWKB(nil, 0)
		assert.NotNil(t, err)
		_, err = GeometryFromWKB([]byte{}, 0)
		assert.NotNil(t, err)
	})

	t.Run("trailing bytes are rejected", func(t *testing.T) {
		withTrailer := append(seattlePoint(t).WKB(), 0xff)

		_, err := GeometryFromWKB(withTrailer, 4326)
		assert.NotNil(t, err)
	})

	t.Run("truncated input is rejected", func(t *testing.T) {
		wkb := seattlePoint(t).WKB()

		_, err := GeometryFromWKB(wkb[:len(wkb)-4], 4326)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "truncated WKB input")
		assert.True(t, errors.Is(err, io.ErrUnexpectedEOF))

		_, err = GeometryFromWKB(wkb[:1], 4326)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "truncated WKB input")
		assert.True(t, errors.Is(err, io.EOF))
	})

	t.Run("an empty point is only a point or a multi-point element", func(t *testing.T) {
		// A line string of two vertices whose coordinates are all NaN.
		lineWithEmptyVertices := "00" + "00000002" + "00000002" +
			"7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000"
		wkb, err := hex.DecodeString(lineWithEmptyVertices)
		assert.Nil(t, err)
		assert.Equal(t, 41, len(wkb))

		// The vertex is rejected for being an empty point rather than for its coordinates.
		_, err = GeometryFromWKB(wkb, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(),
			"a linestring vertex cannot be an empty point; a vertex must be a real coordinate")

		// A polygon of one ring that declares four vertices whose coordinates are all NaN, so
		// the ring carries enough points to reach the vertex check.
		ringWithEmptyVertices := "00" + "00000003" + "00000001" + "00000004" +
			"7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000" +
			"7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000" + "7ff8000000000000"
		wkb, err = hex.DecodeString(ringWithEmptyVertices)
		assert.Nil(t, err)
		assert.Equal(t, 77, len(wkb))

		_, err = GeometryFromWKB(wkb, 0)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(),
			"a ring vertex cannot be an empty point; a vertex must be a real coordinate")

		multiPointWithEmpty, err := hex.DecodeString("00" + "00000004" + "00000001" +
			"00" + "00000001" + "7ff8000000000000" + "7ff8000000000000")
		assert.Nil(t, err)
		read, err := GeometryFromWKB(multiPointWithEmpty, 0)
		assert.Nil(t, err)
		assert.True(t, mustMultiPoint(t, mustEmptyPoint(t, Model.Cartesian)).Equal(read))
	})

	t.Run("the WKB PostGIS writes by default reads back as the polygon", func(t *testing.T) {
		polygon := postgisPolygon(t)

		// The form PostGIS writes for ST_AsBinary with its default byte order.
		asBinary, err := hex.DecodeString("01" + "03000000" + "01000000" + "05000000" +
			"0000000000002440" + "0000000000003440" +
			"0000000000003e40" + "0000000000003440" +
			"0000000000003e40" + "0000000000004440" +
			"0000000000002440" + "0000000000004440" +
			"0000000000002440" + "0000000000003440")
		assert.Nil(t, err)
		assert.Equal(t, 93, len(asBinary))

		read, err := GeometryFromWKB(asBinary, 4326)
		assert.Nil(t, err)
		assert.True(t, polygon.Equal(read), "%s did not read back equal", polygon)
		assert.Equal(t, int32(4326), read.Srid())
		assert.Equal(t, Model.Geographic, read.Model())
	})

	t.Run("the polygon writes the WKB PostGIS writes in XDR byte order", func(t *testing.T) {
		polygon := postgisPolygon(t)

		// The form PostGIS writes for ST_AsBinary when asked for XDR byte order.
		xdr, err := hex.DecodeString("00" + "00000003" + "00000001" + "00000005" +
			"4024000000000000" + "4034000000000000" +
			"403e000000000000" + "4034000000000000" +
			"403e000000000000" + "4044000000000000" +
			"4024000000000000" + "4044000000000000" +
			"4024000000000000" + "4034000000000000")
		assert.Nil(t, err)
		assert.Equal(t, 93, len(xdr))

		assert.Equal(t, xdr, polygon.WKB())
	})

	t.Run("the EWKB PostGIS writes is rejected", func(t *testing.T) {
		// The form PostGIS writes for ST_AsEWKB, which carries an SRID flag.
		ewkb, err := hex.DecodeString("01" + "03000020" + "e6100000" + "01000000" + "05000000" +
			"0000000000002440" + "0000000000003440" +
			"0000000000003e40" + "0000000000003440" +
			"0000000000003e40" + "0000000000004440" +
			"0000000000002440" + "0000000000004440" +
			"0000000000002440" + "0000000000003440")
		assert.Nil(t, err)
		assert.Equal(t, 97, len(ewkb))

		_, err = GeometryFromWKB(ewkb, 4326)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "EWKB format detected")
	})
}
