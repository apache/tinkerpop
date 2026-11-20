#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Globalization;
using Gremlin.Net.Structure.Geo;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.Geo
{
    public class GeometryTests
    {
        private static Point Geographic(double x, double y) => new(x, y, Model.Geographic);

        private static Polygon PolygonWithHole() => new(
            new LineString(Geographic(0, 0), Geographic(10, 0), Geographic(10, 10), Geographic(0, 10),
                Geographic(0, 0)),
            new LineString(Geographic(2, 2), Geographic(2, 4), Geographic(4, 4), Geographic(2, 2)));

        private static GeometryCollection NestedCollection() => new(
            new Point(1, 2),
            new GeometryCollection(new LineString(new Point(0, 0), new Point(1, 1))),
            Point.Empty());

        private static LineString TwoPointLine() => new(new Point(0, 0), new Point(1, 1));

        private static LineString ClosedRing() => new(new Point(0, 0), new Point(10, 0), new Point(10, 10),
            new Point(0, 0));

        [Fact]
        public void PointShouldExposeItsCoordinatesAndModel()
        {
            var point = Geographic(-122.3321, 47.6062);

            Assert.Equal(-122.3321, point.X);
            Assert.Equal(47.6062, point.Y);
            Assert.Null(point.Z);
            Assert.False(point.HasZ);
            Assert.False(point.IsEmpty);
            Assert.Equal(4326, point.Srid);
            Assert.Equal(Model.Geographic, point.Model);
        }

        [Fact]
        public void PointShouldExposeItsZCoordinate()
        {
            var point = new Point(1, 2, 3);

            Assert.True(point.HasZ);
            Assert.Equal(3.0, point.Z!.Value);
            Assert.Equal(Model.Cartesian, point.Model);
            Assert.Equal(0, point.Srid);
        }

        [Fact]
        public void EmptyPointShouldRejectCoordinateAccess()
        {
            var point = Point.Empty();

            Assert.True(point.IsEmpty);
            Assert.Throws<InvalidOperationException>(() => _ = point.X);
            Assert.Throws<InvalidOperationException>(() => _ = point.Y);
            Assert.Throws<InvalidOperationException>(() => _ = point.Z);
        }

        [Theory]
        [InlineData(double.NaN, 1.0)]
        [InlineData(1.0, double.NaN)]
        [InlineData(double.PositiveInfinity, 1.0)]
        [InlineData(1.0, double.NegativeInfinity)]
        public void PointShouldRejectCoordinatesThatAreNotFinite(double x, double y)
        {
            Assert.Throws<ArgumentException>(() => new Point(x, y));
        }

        [Theory]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        public void PointShouldRejectAZCoordinateThatIsNotFinite(double z)
        {
            Assert.Throws<ArgumentException>(() => new Point(1, 2, z));
        }

        [Theory]
        [InlineData(180.5, 0.0)]
        [InlineData(-180.5, 0.0)]
        [InlineData(0.0, 90.5)]
        [InlineData(0.0, -90.5)]
        public void GeographicPointShouldRejectCoordinatesOutsideTheEarth(double x, double y)
        {
            Assert.Throws<ArgumentException>(() => Geographic(x, y));
        }

        [Fact]
        public void GeographicPointShouldAcceptTheRangeBoundaries()
        {
            Assert.Equal(180.0, Geographic(180, 90).X);
            Assert.Equal(-90.0, Geographic(-180, -90).Y);
        }

        [Fact]
        public void GeometryOfAnotherSpatialReferenceSystemShouldReportTheOtherModel()
        {
            var geometry = Geometry.FromWkb(new Point(1, 2).ToWkb(), 3857);

            Assert.Equal(3857, geometry.Srid);
            Assert.Equal(Model.Other, geometry.Model);
            Assert.Equal("POINT(1 2)", geometry.ToWkt());
        }

        [Fact]
        public void ConstructorsShouldRejectTheOtherModel()
        {
            Assert.Throws<ArgumentException>(() => new Point(1, 2, Model.Other));
            Assert.Throws<ArgumentException>(() => Point.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => LineString.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => Polygon.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => MultiPoint.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => MultiLineString.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => MultiPolygon.Empty(Model.Other));
            Assert.Throws<ArgumentException>(() => GeometryCollection.Empty(Model.Other));
        }

        [Fact]
        public void PointWithSridShouldCarryThatSrid()
        {
            var point = Point.WithSrid(1, 2, 3857);

            Assert.Equal(1.0, point.X);
            Assert.Equal(2.0, point.Y);
            Assert.False(point.HasZ);
            Assert.Null(point.Z);
            Assert.False(point.IsEmpty);
            Assert.Equal(3857, point.Srid);
            Assert.Equal(Model.Other, point.Model);
        }

        [Fact]
        public void PointWithSridShouldCarryThatSridWithAZCoordinate()
        {
            var point = Point.WithSrid(1, 2, 3, 3857);

            Assert.Equal(1.0, point.X);
            Assert.Equal(2.0, point.Y);
            Assert.True(point.HasZ);
            Assert.Equal(3.0, point.Z!.Value);
            Assert.Equal(3857, point.Srid);
            Assert.Equal(Model.Other, point.Model);
        }

        [Fact]
        public void PointWithSridShouldApplyTheGeographicRangeForSrid4326()
        {
            Assert.Throws<ArgumentException>(() => Point.WithSrid(181, 0, 4326));
            Assert.Equal(181.0, Point.WithSrid(181, 0, 3857).X);
        }

        [Theory]
        [InlineData(double.NaN, 1.0)]
        [InlineData(1.0, double.NaN)]
        public void PointWithSridShouldRejectCoordinatesThatAreNotFinite(double x, double y)
        {
            Assert.Throws<ArgumentException>(() => Point.WithSrid(x, y, 3857));
            Assert.Throws<ArgumentException>(() => Point.WithSrid(x, y, 0, 3857));
        }

        [Fact]
        public void PointWithSridShouldRejectAZCoordinateThatIsNotFinite()
        {
            Assert.Throws<ArgumentException>(() => Point.WithSrid(1, 2, double.NaN, 3857));
        }

        [Fact]
        public void ThreeNumbersShouldBeACoordinateAndNotAnSrid()
        {
            var withSrid = Point.WithSrid(1, 2, 3);

            Assert.False(withSrid.HasZ);
            Assert.Equal(3, withSrid.Srid);
        }

        [Fact]
        public void EmptyWithAnSridShouldCarryThatSridOnEveryKind()
        {
            var point = Point.Empty(3857);
            var lineString = LineString.Empty(3857);
            var polygon = Polygon.Empty(3857);
            var multiPoint = MultiPoint.Empty(3857);
            var multiLineString = MultiLineString.Empty(3857);
            var multiPolygon = MultiPolygon.Empty(3857);
            var collection = GeometryCollection.Empty(3857);

            var empties = new Geometry[]
            {
                point, lineString, polygon, multiPoint, multiLineString, multiPolygon, collection
            };
            foreach (var empty in empties)
            {
                Assert.True(empty.IsEmpty);
                Assert.False(empty.HasZ);
                Assert.Equal(3857, empty.Srid);
                Assert.Equal(Model.Other, empty.Model);
            }

            Assert.IsType<Point>(point);
            Assert.IsType<LineString>(lineString);
            Assert.IsType<Polygon>(polygon);
            Assert.IsType<MultiPoint>(multiPoint);
            Assert.IsType<MultiLineString>(multiLineString);
            Assert.IsType<MultiPolygon>(multiPolygon);
            Assert.IsType<GeometryCollection>(collection);
        }

        [Fact]
        public void EmptyWithTheSridOfAModelShouldMatchEmptyWithThatModel()
        {
            Assert.Equal(Point.Empty(Model.Cartesian), Point.Empty(0));
            Assert.Equal(Point.Empty(Model.Geographic), Point.Empty(4326));
        }

        [Fact]
        public void LineStringShouldTakeTheSridOfItsPoints()
        {
            var lineString = new LineString(Point.WithSrid(0, 0, 3857), Point.WithSrid(1, 1, 3857));

            Assert.Equal(3857, lineString.Srid);
            Assert.Equal(Model.Other, lineString.Model);
            Assert.Equal("LINESTRING(0 0, 1 1)", lineString.ToWkt());
        }

        [Fact]
        public void LineStringShouldNeedTwoPoints()
        {
            Assert.Throws<ArgumentException>(() => new LineString(new Point(1, 2)));
            Assert.Throws<ArgumentException>(() => new LineString());

            var empty = LineString.Empty();

            Assert.True(empty.IsEmpty);
            Assert.Empty(empty.Points);
        }

        [Fact]
        public void LineStringShouldRejectMixedDimensions()
        {
            Assert.Throws<ArgumentException>(() => new LineString(new Point(0, 0), new Point(1, 1, 1)));
        }

        [Fact]
        public void LineStringShouldRejectAnEmptyPointAsAVertex()
        {
            Assert.Throws<ArgumentException>(() => new LineString(new Point(0, 0), Point.Empty()));
        }

        [Fact]
        public void PolygonShouldRejectRingsThatAreNotUsable()
        {
            var shell = new LineString(new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 0));

            Assert.Throws<ArgumentException>(() => new Polygon(shell, LineString.Empty()));
            Assert.Throws<ArgumentException>(() => new Polygon(LineString.Empty(), shell));
            Assert.Throws<ArgumentException>(() =>
                new Polygon(new LineString(new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1))));
            // A ring that is closed but holds only 3 points is still too short.
            Assert.Throws<ArgumentException>(() =>
                new Polygon(new LineString(new Point(0, 0), new Point(1, 0), new Point(0, 0))));
        }

        [Fact]
        public void CollectionsShouldRejectPartsOfDifferentSpatialReferenceSystems()
        {
            Assert.Throws<ArgumentException>(() => new MultiPoint(new Point(1, 2), Geographic(1, 2)));
            Assert.Throws<ArgumentException>(() => new GeometryCollection(new Point(1, 2), Geographic(1, 2)));
        }

        [Fact]
        public void CollectionsShouldNeedAtLeastOnePart()
        {
            Assert.Throws<ArgumentException>(() => new MultiPoint());
            Assert.Throws<ArgumentException>(() => new MultiLineString());
            Assert.Throws<ArgumentException>(() => new MultiPolygon());
            Assert.Throws<ArgumentException>(() => new GeometryCollection());
        }

        [Fact]
        public void ConstructorsShouldRejectANullCollection()
        {
            Assert.Throws<ArgumentNullException>(() => new LineString((Point[]) null!));
            Assert.Throws<ArgumentNullException>(() => new MultiPoint((Point[]) null!));
            Assert.Throws<ArgumentNullException>(() => new MultiLineString((LineString[]) null!));
            Assert.Throws<ArgumentNullException>(() => new MultiPolygon((Polygon[]) null!));
            Assert.Throws<ArgumentNullException>(() => new GeometryCollection((Geometry[]) null!));
            Assert.Throws<ArgumentNullException>(() => new Polygon(ClosedRing(), (LineString[]) null!));
        }

        [Fact]
        public void ConstructorsShouldRejectANullPart()
        {
            Assert.Throws<ArgumentNullException>(() => new LineString(new Point(0, 0), null!));
            Assert.Throws<ArgumentNullException>(() => new MultiPoint(new Point(1, 2), null!));
            Assert.Throws<ArgumentNullException>(() => new MultiLineString(TwoPointLine(), null!));
            Assert.Throws<ArgumentNullException>(() => new MultiPolygon(new Polygon(ClosedRing()), null!));
            Assert.Throws<ArgumentNullException>(() => new GeometryCollection(new Point(1, 2), null!));
            Assert.Throws<ArgumentNullException>(() => new Polygon(ClosedRing(), (LineString) null!));
        }

        [Fact]
        public void ConstructorsShouldCopyTheArrayTheyAreGiven()
        {
            var points = new[] { new Point(0, 0), new Point(1, 1) };
            var lineString = new LineString(points);
            var multiPoint = new MultiPoint(points);
            var hole = new LineString(new Point(2, 2), new Point(2, 4), new Point(4, 4), new Point(2, 2));
            var holes = new[] { hole };
            var polygon = new Polygon(ClosedRing(), holes);

            points[1] = new Point(9, 9);
            holes[0] = new LineString(new Point(3, 3), new Point(3, 5), new Point(5, 5), new Point(3, 3));

            Assert.Equal(new Point(1, 1), lineString.Points[1]);
            Assert.Equal(new Point(1, 1), multiPoint.Points[1]);
            Assert.Equal(hole, polygon.InteriorRings[0]);
        }

        [Fact]
        public void MultiPointShouldExposeItsPointsWithoutAllowingChanges()
        {
            var first = new Point(1, 2);
            var second = new Point(3, 4);
            var multiPoint = new MultiPoint(first, second);

            Assert.Equal(new[] { first, second }, multiPoint.Points);
            Assert.Equal(new Geometry[] { first, second }, multiPoint.Geometries);
            Assert.Throws<NotSupportedException>(() => ((IList<Point>) multiPoint.Points).Add(new Point(5, 6)));
        }

        [Fact]
        public void CollectionNestingShouldBeLimited()
        {
            Geometry nested = new Point(1, 2);
            for (var level = 0; level < 32; level++)
            {
                nested = new GeometryCollection(nested);
            }

            Assert.False(nested.IsEmpty);
            Assert.Throws<ArgumentException>(() => new GeometryCollection(nested));
        }

        [Fact]
        public void GeometriesOfTheSameValueShouldBeEqual()
        {
            Assert.Equal(new Point(1, 2), new Point(1, 2));
            Assert.Equal(new Point(1, 2).GetHashCode(), new Point(1, 2).GetHashCode());
            Assert.Equal(PolygonWithHole(), PolygonWithHole());
            Assert.Equal(PolygonWithHole().GetHashCode(), PolygonWithHole().GetHashCode());
            Assert.Equal(NestedCollection(), NestedCollection());
            Assert.Equal(NestedCollection().GetHashCode(), NestedCollection().GetHashCode());
        }

        [Fact]
        public void NegativeZeroShouldEqualZero()
        {
            var negative = new Point(-0.0, 0.0);
            var positive = new Point(0.0, 0.0);

            Assert.Equal(positive, negative);
            Assert.Equal(positive.GetHashCode(), negative.GetHashCode());
        }

        [Fact]
        public void EmptyPointsOfTheSameSridShouldBeEqual()
        {
            Assert.Equal(Point.Empty(), Point.Empty());
            Assert.Equal(Point.Empty().GetHashCode(), Point.Empty().GetHashCode());
        }

        [Fact]
        public void GeometriesOfDifferentValuesShouldNotBeEqual()
        {
            Assert.NotEqual(new Point(1, 2), Geographic(1, 2));
            Assert.NotEqual<Geometry>(new Point(1, 2), new Point(1, 2, 3));
            Assert.NotEqual(new Point(1, 2, 3), new Point(1, 2, 4));
            Assert.NotEqual<Geometry>(new MultiPoint(new Point(1, 2)), new GeometryCollection(new Point(1, 2)));
            Assert.NotEqual<Geometry>(new GeometryCollection(new Point(1, 2)), new MultiPoint(new Point(1, 2)));
            Assert.NotEqual(new Point(1, 2), new Point(2, 1));
            Assert.NotEqual(Point.Empty(), new Point(1, 2));
            Assert.NotEqual(new Point(1, 2), Point.Empty());
            Assert.NotEqual(PolygonWithHole(), new Polygon(PolygonWithHole().ExteriorRing));
            Assert.NotEqual(new MultiPoint(new Point(1, 2), new Point(3, 4)), new MultiPoint(new Point(1, 2)));
            Assert.NotEqual(new MultiPoint(new Point(1, 2)), new MultiPoint(new Point(1, 2), new Point(3, 4)));
        }

        [Fact]
        public void EmptyGeometriesOfDifferentSpatialReferenceSystemsShouldNotBeEqual()
        {
            Assert.NotEqual(Point.Empty(), Point.Empty(3857));
            Assert.NotEqual(LineString.Empty(), LineString.Empty(3857));
            Assert.NotEqual(Polygon.Empty(), Polygon.Empty(3857));
            Assert.NotEqual(MultiPoint.Empty(), MultiPoint.Empty(3857));
            Assert.NotEqual(MultiLineString.Empty(), MultiLineString.Empty(3857));
            Assert.NotEqual(MultiPolygon.Empty(), MultiPolygon.Empty(3857));
            Assert.NotEqual(GeometryCollection.Empty(), GeometryCollection.Empty(3857));
        }

        [Fact]
        public void EqualityOperatorsShouldAgreeWithEquals()
        {
            var point = new Point(1, 2);
            var same = new Point(1, 2);
            var other = new Point(3, 4);
            Point? none = null;

            Assert.True(point == same);
            Assert.False(point != same);
            Assert.True(point != other);
            Assert.False(point == other);
            Assert.False(point == none);
            Assert.True(point != none);
            Assert.True(none == null);
            Assert.False(point.Equals(null));
        }

        [Fact]
        public void WktShouldDescribeTheGeometry()
        {
            Assert.Equal("POINT(-122.3321 47.6062)", Geographic(-122.3321, 47.6062).ToWkt());
            Assert.Equal("POINT Z (1 2 3)", new Point(1, 2, 3).ToWkt());
            Assert.Equal("POINT EMPTY", Point.Empty().ToWkt());
            Assert.Equal("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))", PolygonWithHole().ToWkt());
            Assert.Equal("GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)",
                NestedCollection().ToWkt());
            Assert.Equal("LINESTRING EMPTY", LineString.Empty().ToWkt());
            Assert.Equal("MULTIPOINT((1 2), EMPTY)", new MultiPoint(new Point(1, 2), Point.Empty()).ToWkt());
            // A collection whose only part is empty still writes a body, unlike a collection with no parts.
            Assert.Equal("MULTIPOINT(EMPTY)", new MultiPoint(Point.Empty()).ToWkt());
            Assert.Equal("GEOMETRYCOLLECTION(POINT EMPTY)", new GeometryCollection(Point.Empty()).ToWkt());
            Assert.Equal("MULTIPOINT EMPTY", MultiPoint.Empty().ToWkt());
            Assert.Equal("GEOMETRYCOLLECTION EMPTY", GeometryCollection.Empty().ToWkt());
            Assert.Equal("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
                new MultiLineString(new LineString(new Point(0, 0), new Point(1, 1)),
                    new LineString(new Point(2, 2), new Point(3, 3))).ToWkt());
            Assert.Equal("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
                new MultiPolygon(
                    new Polygon(new LineString(Geographic(0, 0), Geographic(1, 0), Geographic(1, 1),
                        Geographic(0, 1), Geographic(0, 0))),
                    new Polygon(new LineString(Geographic(2, 2), Geographic(3, 2), Geographic(3, 3),
                        Geographic(2, 3), Geographic(2, 2)))).ToWkt());
        }

        [Fact]
        public void EwktShouldCarryTheSrid()
        {
            Assert.Equal("SRID=4326;POINT(1 2)", Geographic(1, 2).ToEwkt());
        }

        [Fact]
        public void StringFormShouldBeTheWkt()
        {
            var polygon = PolygonWithHole();

            Assert.Equal(polygon.ToWkt(), polygon.ToString());
        }

        [Theory]
        [InlineData(0.1, "0.1")]
        [InlineData(1e15, "1000000000000000")]
        [InlineData(1e16, "10000000000000000")]
        [InlineData(1e21, "1000000000000000000000")]
        [InlineData(1e-5, "0.00001")]
        [InlineData(1e-6, "0.000001")]
        [InlineData(1e-7, "0.0000001")]
        [InlineData(-0.0, "-0")]
        [InlineData(-1.5e-7, "-0.00000015")]
        public void WktShouldWriteCoordinatesAsPlainDecimals(double value, string expected)
        {
            Assert.Equal($"POINT({expected} 0)", new Point(value, 0).ToWkt());
        }

        [Fact]
        public void WktShouldWriteCoordinatesWithADecimalPointUnderAnyCulture()
        {
            var culture = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");

                Assert.Equal("POINT(0.1 0)", new Point(0.1, 0).ToWkt());
                Assert.Equal("POINT(0.0000001 0)", new Point(1e-7, 0).ToWkt());
            }
            finally
            {
                CultureInfo.CurrentCulture = culture;
            }
        }

        [Fact]
        public void WkbShouldBeBigEndianIsoWkb()
        {
            var expected = new byte[]
            {
                0x00, 0x00, 0x00, 0x00, 0x01, 0xc0, 0x5e, 0x95, 0x41, 0x20, 0x5b, 0xc0, 0x1a, 0x40, 0x47, 0xcd,
                0x97, 0xf6, 0x2b, 0x6a, 0xe8
            };

            Assert.Equal(expected, Geographic(-122.3321, 47.6062).ToWkb());
        }

        [Fact]
        public void WkbOfAnEmptyPointShouldHoldNanCoordinates()
        {
            var expected = new byte[]
            {
                0x00, 0x00, 0x00, 0x00, 0x01, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7f, 0xf8, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00
            };

            Assert.Equal(expected, Point.Empty().ToWkb());
        }

        [Fact]
        public void FromWkbShouldRejectInputThatIsNotOneGeometry()
        {
            var wkb = new Point(1, 2).ToWkb();

            // The byte order marker, the point type code and two coordinates.
            Assert.Equal(21, wkb.Length);

            var withTrailingByte = new byte[wkb.Length + 1];
            Array.Copy(wkb, withTrailingByte, wkb.Length);

            Assert.Equal(22, withTrailingByte.Length);

            var trailing = Assert.Throws<ArgumentException>(() => Geometry.FromWkb(withTrailingByte, 0));

            Assert.Contains("Unexpected trailing bytes in WKB input", trailing.Message);

            // 10 bytes end inside the x coordinate, which starts at index 5 and needs 8 bytes.
            var truncated = Assert.Throws<ArgumentException>(() => Geometry.FromWkb(wkb[..10], 0));

            Assert.Contains("Truncated WKB input: unexpected end of input", truncated.Message);

            var empty = Assert.Throws<ArgumentException>(() => Geometry.FromWkb(Array.Empty<byte>(), 0));

            Assert.Contains("WKB input cannot be empty", empty.Message);
            Assert.Throws<ArgumentNullException>(() => Geometry.FromWkb(null!, 0));
        }

        [Fact]
        public void FromWkbShouldReadTheWkbPostgisWritesByDefault()
        {
            // little-endian ISO WKB, as PostGIS ST_AsBinary writes by default
            var asBinary = BytesFromHex(
                "01" + "03000000" + "01000000" + "05000000"
                     + "0000000000002440" + "0000000000003440"
                     + "0000000000003e40" + "0000000000003440"
                     + "0000000000003e40" + "0000000000004440"
                     + "0000000000002440" + "0000000000004440"
                     + "0000000000002440" + "0000000000003440");
            Assert.Equal(93, asBinary.Length);

            var read = Geometry.FromWkb(asBinary, 4326);

            Assert.Equal(PostgisPolygon(), read);
            Assert.Equal(4326, read.Srid);
            Assert.Equal(Model.Geographic, read.Model);
        }

        [Fact]
        public void ToWkbShouldMatchTheFormPostgisWritesForXdr()
        {
            // big-endian ISO WKB, as PostGIS ST_AsBinary writes with XDR
            var xdr = BytesFromHex(
                "00" + "00000003" + "00000001" + "00000005"
                     + "4024000000000000" + "4034000000000000"
                     + "403e000000000000" + "4034000000000000"
                     + "403e000000000000" + "4044000000000000"
                     + "4024000000000000" + "4044000000000000"
                     + "4024000000000000" + "4034000000000000");
            Assert.Equal(93, xdr.Length);

            var polygon = PostgisPolygon();

            Assert.Equal(xdr, polygon.ToWkb());
            Assert.Equal(polygon, Geometry.FromWkb(xdr, 4326));
        }

        [Fact]
        public void FromWkbShouldRejectTheEwkbPostgisWrites()
        {
            // PostGIS ST_AsEWKB, little-endian with an SRID flag and SRID 4326
            var ewkb = BytesFromHex(
                "01" + "03000020" + "e6100000" + "01000000" + "05000000"
                     + "0000000000002440" + "0000000000003440"
                     + "0000000000003e40" + "0000000000003440"
                     + "0000000000003e40" + "0000000000004440"
                     + "0000000000002440" + "0000000000004440"
                     + "0000000000002440" + "0000000000003440");
            Assert.Equal(97, ewkb.Length);

            var exception = Assert.Throws<ArgumentException>(() => Geometry.FromWkb(ewkb, 4326));

            Assert.Contains("EWKB", exception.Message);
        }

        private static Polygon PostgisPolygon() => new(
            new LineString(Geographic(10, 20), Geographic(30, 20), Geographic(30, 40), Geographic(10, 40),
                Geographic(10, 20)));

        private static byte[] BytesFromHex(string hex) => Convert.FromHexString(hex);
    }
}
