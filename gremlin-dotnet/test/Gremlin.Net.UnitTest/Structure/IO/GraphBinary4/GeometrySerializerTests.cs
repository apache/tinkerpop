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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Structure.Geo;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Gremlin.Net.Structure.IO.GraphBinary4.Types;
using GeoModel = Gremlin.Net.Structure.Geo.Model;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class GeometrySerializerTests
    {
        private static readonly GraphBinaryWriter Writer = new();
        private static readonly GraphBinaryReader Reader = new();

        /// <summary>
        /// POINT(-122.3321 47.6062) under SRID 4326, fully qualified: the type code and value flag, the SRID and
        /// 21 bytes of big-endian ISO WKB.
        /// </summary>
        private static readonly byte[] GeographicPointBytes =
        {
            0x31, 0x00, 0x00, 0x00, 0x10, 0xe6, 0x00, 0x00, 0x00, 0x00, 0x01, 0xc0, 0x5e, 0x95, 0x41, 0x20, 0x5b,
            0xc0, 0x1a, 0x40, 0x47, 0xcd, 0x97, 0xf6, 0x2b, 0x6a, 0xe8
        };

        private static Point GeographicPoint() => new(-122.3321, 47.6062, GeoModel.Geographic);

        public static IEnumerable<object[]> Geometries() => new[]
        {
            new object[] { new Point(1, 2) },
            new object[] { new Point(1, 2, 3) },
            new object[] { Point.Empty() },
            new object[] { new LineString(new Point(0, 0), new Point(1, 1), new Point(2, 0)) },
            new object[] { LineString.Empty() },
            new object[]
            {
                new Polygon(
                    new LineString(new Point(0, 0), new Point(10, 0), new Point(10, 10), new Point(0, 10),
                        new Point(0, 0)),
                    new LineString(new Point(2, 2), new Point(2, 4), new Point(4, 4), new Point(2, 2)))
            },
            new object[] { Polygon.Empty() },
            new object[] { new MultiPoint(new Point(1, 2), Point.Empty()) },
            new object[] { MultiPoint.Empty() },
            new object[]
            {
                new MultiLineString(new LineString(new Point(0, 0), new Point(1, 1)),
                    new LineString(new Point(2, 2), new Point(3, 3)))
            },
            new object[] { MultiLineString.Empty() },
            new object[]
            {
                new MultiPolygon(new Polygon(new LineString(new Point(0, 0), new Point(1, 0), new Point(1, 1),
                    new Point(0, 1), new Point(0, 0))))
            },
            new object[] { MultiPolygon.Empty() },
            new object[]
            {
                new GeometryCollection(new Point(1, 2),
                    new GeometryCollection(new LineString(new Point(0, 0), new Point(1, 1))), Point.Empty())
            },
            new object[] { GeometryCollection.Empty() },
            new object[]
            {
                new LineString(new Point(0, 0, 1), new Point(1, 1, 2))
            },
            new object[]
            {
                new Polygon(new LineString(new Point(0, 0, 1), new Point(1, 0, 1), new Point(1, 1, 1),
                    new Point(0, 0, 1)))
            },
            new object[] { new MultiPoint(new Point(1, 2, 3), new Point(4, 5, 6)) },
            new object[]
            {
                new MultiLineString(new LineString(new Point(0, 0, 1), new Point(1, 1, 2)))
            },
            new object[]
            {
                new MultiPolygon(new Polygon(new LineString(new Point(0, 0, 1), new Point(1, 0, 1),
                    new Point(1, 1, 1), new Point(0, 0, 1))))
            },
            new object[] { new GeometryCollection(new Point(1, 2, 3)) }
        };

        public static IEnumerable<object[]> ConcreteGeometryTypes() => new[]
        {
            new object[] { typeof(Point) },
            new object[] { typeof(LineString) },
            new object[] { typeof(Polygon) },
            new object[] { typeof(MultiPoint) },
            new object[] { typeof(MultiLineString) },
            new object[] { typeof(MultiPolygon) },
            new object[] { typeof(GeometryCollection) }
        };

        [Fact]
        public async Task ShouldWriteTheSridFollowedByTheWkb()
        {
            using var stream = new MemoryStream();

            await Writer.WriteAsync(GeographicPoint(), stream);

            Assert.Equal(GeographicPointBytes, stream.ToArray());
        }

        [Fact]
        public async Task ShouldReadTheSridFollowedByTheWkb()
        {
            using var stream = new MemoryStream(GeographicPointBytes);

            var read = await Reader.ReadAsync(stream);

            var point = Assert.IsType<Point>(read);
            Assert.Equal(GeographicPoint(), point);
            Assert.Equal(GeoModel.Geographic, point.Model);
        }

        [Fact]
        public async Task ShouldWriteAndReadNull()
        {
            var serializer = new GeometrySerializer();
            using var writeStream = new MemoryStream();
            await writeStream.WriteByteAsync(serializer.DataType.TypeCode);
            await serializer.WriteAsync(null, writeStream, Writer);

            Assert.Equal(new byte[] { 0x31, 0x01 }, writeStream.ToArray());

            using var readStream = new MemoryStream(writeStream.ToArray());

            Assert.Null(await Reader.ReadAsync(readStream));
        }

        [Fact]
        public async Task ShouldWriteAnEmptyPointAsNanCoordinates()
        {
            using var stream = new MemoryStream();
            await Writer.WriteAsync(Point.Empty(), stream);

            Assert.Equal(new byte[]
            {
                0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x7f, 0xf8, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            }, stream.ToArray());
        }

        [Theory]
        [MemberData(nameof(Geometries))]
        public async Task ShouldRoundTripEveryKind(Geometry geometry)
        {
            var read = await RoundTrip(geometry);

            Assert.Equal(geometry, read);
            Assert.Equal(geometry.GetType(), read.GetType());
            Assert.Equal(geometry.HasZ, read.HasZ);
        }

        [Fact]
        public async Task ShouldReadLittleEndianWkb()
        {
            using var stream = new MemoryStream(LittleEndianGeographicPointBytes());

            var read = await Reader.ReadAsync(stream);

            Assert.Equal(GeographicPoint(), read);
        }

        [Fact]
        public async Task ShouldKeepNegativeZero()
        {
            using var stream = new MemoryStream();
            await Writer.WriteAsync(new Point(-0.0, 0.0), stream);
            var bytes = stream.ToArray();

            // The x coordinate starts at index 11, after the type code, value flag, SRID, byte order and WKB type.
            Assert.Equal(new byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, bytes[11..19]);

            var read = (Point) await RoundTrip(new Point(-0.0, 0.0));

            Assert.True(double.IsNegative(read.X));
        }

        [Fact]
        public async Task ShouldReadGeometryFollowedByAnotherValue()
        {
            using var stream = new MemoryStream();
            await Writer.WriteAsync(GeographicPoint(), stream);
            await Writer.WriteAsync(42, stream);
            stream.Position = 0;

            Assert.Equal(GeographicPoint(), await Reader.ReadAsync(stream));
            Assert.Equal(42, await Reader.ReadAsync(stream));
            Assert.Equal(stream.Length, stream.Position);
        }

        [Fact]
        public async Task ShouldRoundTripGeometriesInsideAList()
        {
            var list = new List<object?> { new Point(1, 2), LineString.Empty(), 42 };
            using var stream = new MemoryStream();
            await Writer.WriteAsync(list, stream);
            stream.Position = 0;

            var read = Assert.IsType<List<object>>(await Reader.ReadAsync(stream));

            Assert.Equal(3, read.Count);
            Assert.Equal(new Point(1, 2), read[0]);
            Assert.Equal(LineString.Empty(), read[1]);
            Assert.Equal(42, read[2]);
        }

        [Theory]
        // Truncated inside the y coordinate, which a multi-byte read reports, and right after the SRID, which the
        // single-byte read of the byte order marker reports.
        [InlineData(4)]
        [InlineData(21)]
        public async Task ShouldFailOnAValueThatEndsInsideTheWkb(int missingBytes)
        {
            using var stream = new MemoryStream(GeographicPointBytes[..^missingBytes]);

            var exception = await Assert.ThrowsAsync<IOException>(() => Reader.ReadAsync(stream));

            Assert.Equal("The geometry value ends before its WKB is complete", exception.Message);
        }

        [Fact]
        public async Task ShouldFailOnAPointCountThatExceedsTheInput()
        {
            // A line string that declares 0x7fffffff points but holds one.
            var wkb = new List<byte> { 0x00, 0x00, 0x00, 0x00, 0x02, 0x7f, 0xff, 0xff, 0xff };
            wkb.AddRange(new byte[16]);

            await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));
        }

        [Theory]
        // A point type code carrying the EWKB Z flag, the M flag and the SRID flag in turn.
        [InlineData(unchecked((int) 0x80000001))]
        [InlineData(0x40000001)]
        [InlineData(0x20000001)]
        public async Task ShouldFailOnEwkb(int typeCode)
        {
            var wkb = new byte[5];
            BinaryPrimitives.WriteInt32BigEndian(wkb.AsSpan(1), typeCode);

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb, 0));

            Assert.Contains("EWKB", exception.Message);
        }

        [Fact]
        public async Task ShouldFailOnAByteOrderMarkerThatIsNotValid()
        {
            // A point whose byte order marker is neither 0x00 for big-endian nor 0x01 for little-endian.
            var wkb = new List<byte> { 0x02 };
            wkb.AddRange(BigEndianIntOf(1));
            wkb.AddRange(BitsOf(1.0));
            wkb.AddRange(BitsOf(2.0));

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));

            Assert.Contains("Invalid WKB byte order", exception.Message);
        }

        [Fact]
        public async Task ShouldFailOnAMixOfTwoAndThreeDimensionalGeometries()
        {
            // A 2D multi-point holding a 3D point.
            var wkb = new List<byte> { 0x00 };
            wkb.AddRange(BigEndianIntOf(4));
            wkb.AddRange(BigEndianIntOf(1));
            wkb.Add(0x00);
            wkb.AddRange(BigEndianIntOf(1001));
            wkb.AddRange(BitsOf(1.0));
            wkb.AddRange(BitsOf(2.0));
            wkb.AddRange(BitsOf(3.0));

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));

            Assert.Contains("Mixed Z dimensions in WKB", exception.Message);
        }

        [Theory]
        // A ring of no points, and a ring of 3 points that are all present, so the point count is rejected on its
        // own rather than through the input ending early.
        [InlineData(0)]
        [InlineData(3)]
        public async Task ShouldFailOnAPolygonRingThatHoldsTooFewPoints(int pointCount)
        {
            // A polygon that declares one ring of the given point count and holds that many vertices.
            var wkb = new List<byte> { 0x00 };
            wkb.AddRange(BigEndianIntOf(3));
            wkb.AddRange(BigEndianIntOf(1));
            wkb.AddRange(BigEndianIntOf(pointCount));
            for (var index = 0; index < pointCount; index++)
            {
                wkb.AddRange(BitsOf(index));
                wkb.AddRange(BitsOf(index + 1));
            }

            // The byte order marker, the polygon type code, the ring count, the point count and 16 bytes a vertex.
            Assert.Equal(13 + 16 * pointCount, wkb.Count);

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));

            Assert.Contains("Polygon ring must have at least 4 points", exception.Message);
        }

        [Fact]
        public async Task ShouldFailOnAPointCountThatDoesNotFitAnInt32()
        {
            // A line string that declares 0xffffffff points.
            var wkb = new List<byte> { 0x00 };
            wkb.AddRange(BigEndianIntOf(2));
            wkb.AddRange(new byte[] { 0xff, 0xff, 0xff, 0xff });

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));

            Assert.Contains("Invalid point count: 4294967295", exception.Message);
        }

        [Fact]
        public async Task ShouldFailOnACollectionPartOfTheWrongKind()
        {
            var multiPointOfALineString = await Assert.ThrowsAnyAsync<IOException>(
                () => ReadValue(CollectionOf(4, BigEndianLineString()), 0));

            Assert.Contains("Expected Point in MultiPoint, got LineString", multiPointOfALineString.Message);

            var multiLineStringOfAPoint = await Assert.ThrowsAnyAsync<IOException>(
                () => ReadValue(CollectionOf(5, BigEndianPoint(1, 2)), 0));

            Assert.Contains("Expected LineString in MultiLineString, got Point", multiLineStringOfAPoint.Message);

            var multiPolygonOfALineString = await Assert.ThrowsAnyAsync<IOException>(
                () => ReadValue(CollectionOf(6, BigEndianLineString()), 0));

            Assert.Contains("Expected Polygon in MultiPolygon, got LineString", multiPolygonOfALineString.Message);
        }

        [Theory]
        [InlineData(2001, "M dimension")]
        [InlineData(3001, "ZM dimension")]
        [InlineData(8, "Unknown WKB type code")]
        public async Task ShouldFailOnAWkbTypeThatIsNotSupported(int typeCode, string expectedMessage)
        {
            var wkb = new byte[5];
            BinaryPrimitives.WriteInt32BigEndian(wkb.AsSpan(1), typeCode);

            var exception = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb, 0));

            Assert.Contains(expectedMessage, exception.Message);
        }

        [Fact]
        public async Task ShouldReadTheByteOrderOfEveryNestedGeometry()
        {
            var expected = new MultiPoint(new Point(1, 2), new Point(3, 4));

            // A little-endian multi-point header holding two big-endian points.
            var littleEndianHeader = new List<byte> { 0x01 };
            littleEndianHeader.AddRange(LittleEndianIntOf(4));
            littleEndianHeader.AddRange(LittleEndianIntOf(2));
            littleEndianHeader.AddRange(BigEndianPoint(1, 2));
            littleEndianHeader.AddRange(BigEndianPoint(3, 4));

            Assert.Equal(expected, await ReadValue(littleEndianHeader.ToArray(), 0));

            // A big-endian multi-point header holding two little-endian points.
            var bigEndianHeader = new List<byte> { 0x00 };
            bigEndianHeader.AddRange(BigEndianIntOf(4));
            bigEndianHeader.AddRange(BigEndianIntOf(2));
            bigEndianHeader.AddRange(LittleEndianPoint(1, 2));
            bigEndianHeader.AddRange(LittleEndianPoint(3, 4));

            Assert.Equal(expected, await ReadValue(bigEndianHeader.ToArray(), 0));
        }

        [Fact]
        public async Task ShouldReadAThreeDimensionalEmptyPoint()
        {
            // A point of type 1001 whose x, y and z are all NaN.
            var wkb = new List<byte> { 0x00, 0x00, 0x00, 0x03, 0xe9 };
            for (var ordinate = 0; ordinate < 3; ordinate++)
            {
                wkb.AddRange(new byte[] { 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            }

            var point = Assert.IsType<Point>(await ReadValue(wkb.ToArray(), 0));

            Assert.True(point.IsEmpty);
            Assert.True(point.HasZ);
            Assert.Equal("POINT Z EMPTY", point.ToWkt());
            Assert.Equal(wkb.ToArray(), point.ToWkb());
        }

        [Fact]
        public async Task ShouldFailOnACoordinateThatIsNotUsable()
        {
            // A line string whose first vertex holds NaN, which encodes an empty point.
            var nanVertex = new List<byte> { 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02 };
            nanVertex.AddRange(new byte[] { 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            nanVertex.AddRange(new byte[] { 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            nanVertex.AddRange(new byte[16]);

            var emptyVertex = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(nanVertex.ToArray(), 0));

            Assert.Contains("A linestring vertex cannot be an empty point", emptyVertex.Message);

            // A point whose x is NaN while its y is a real coordinate, which is not an empty point.
            var halfNan = new List<byte> { 0x00, 0x00, 0x00, 0x00, 0x01 };
            halfNan.AddRange(new byte[] { 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            halfNan.AddRange(BitsOf(5.0));

            var notACoordinate = await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(halfNan.ToArray(), 0));

            Assert.Contains("x coordinate cannot be NaN or infinite", notACoordinate.Message);

            // A point at longitude 181, outside the geographic range.
            var outOfRange = new List<byte> { 0x00, 0x00, 0x00, 0x00, 0x01 };
            outOfRange.AddRange(BitsOf(181.0));
            outOfRange.AddRange(BitsOf(0.0));

            var outsideTheEarth = await Assert.ThrowsAnyAsync<IOException>(
                () => ReadValue(outOfRange.ToArray(), 4326));

            Assert.Contains("GEOGRAPHIC longitude must be in [-180, 180]", outsideTheEarth.Message);
        }

        [Fact]
        public async Task ShouldFailOnNestingThatIsTooDeep()
        {
            var wkb = new List<byte>();
            for (var level = 0; level < 33; level++)
            {
                wkb.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x01 });
            }

            wkb.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01 });
            wkb.AddRange(BitsOf(1.0));
            wkb.AddRange(BitsOf(2.0));

            await Assert.ThrowsAnyAsync<IOException>(() => ReadValue(wkb.ToArray(), 0));
        }

        [Fact]
        public async Task ShouldReadFromABufferThatRefillsInsideTheWkb()
        {
            using var source = new MemoryStream(GeographicPointBytes);
            // A buffer smaller than the value, so the WKB is read across several refills.
            await using var buffer = new GraphBinaryReadBuffer(source, 8);

            var read = await Reader.ReadAsync(buffer);

            Assert.Equal(GeographicPoint(), read);
        }

        [Theory]
        [MemberData(nameof(ConcreteGeometryTypes))]
        public void ShouldUseTheGeometrySerializerForEveryKind(Type geometryType)
        {
            var serializer = TypeSerializerRegistry.Instance.GetSerializerFor(geometryType);

            Assert.IsType<GeometrySerializer>(serializer);
            Assert.Equal(DataType.Geometry, serializer.DataType);
        }

        private static async Task<Geometry> RoundTrip(Geometry geometry)
        {
            using var stream = new MemoryStream();
            await Writer.WriteAsync(geometry, stream);
            stream.Position = 0;
            var read = await Reader.ReadAsync(stream);
            Assert.Equal(stream.Length, stream.Position);
            return Assert.IsAssignableFrom<Geometry>(read);
        }

        /// <summary>
        /// Reads a geometry from the value bytes of the GraphBinary format: the SRID and then the given WKB.
        /// </summary>
        private static async Task<object?> ReadValue(byte[] wkb, int srid)
        {
            using var stream = new MemoryStream();
            await stream.WriteIntAsync(srid);
            await stream.WriteAsync(wkb, 0, wkb.Length);
            stream.Position = 0;
            var read = await new GeometrySerializer().ReadNonNullableValueAsync(stream, Reader);
            // A geometry that is read consumes exactly its own bytes.
            Assert.Equal(stream.Length, stream.Position);
            return read;
        }

        private static byte[] LittleEndianGeographicPointBytes()
        {
            var bytes = new List<byte> { DataType.Geometry.TypeCode, 0x00, 0x00, 0x00, 0x10, 0xe6, 0x01 };
            var typeCode = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(typeCode, 1);
            bytes.AddRange(typeCode);
            bytes.AddRange(LittleEndianBitsOf(-122.3321));
            bytes.AddRange(LittleEndianBitsOf(47.6062));
            return bytes.ToArray();
        }

        private static byte[] BitsOf(double value)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(bytes, value);
            return bytes;
        }

        private static byte[] LittleEndianBitsOf(double value)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(bytes, value);
            return bytes;
        }

        private static byte[] BigEndianIntOf(int value)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            return bytes;
        }

        private static byte[] LittleEndianIntOf(int value)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
            return bytes;
        }

        /// <summary>
        /// The WKB of a 2D point in big-endian byte order, including its own byte order marker.
        /// </summary>
        private static byte[] BigEndianPoint(double x, double y)
        {
            var bytes = new List<byte> { 0x00 };
            bytes.AddRange(BigEndianIntOf(1));
            bytes.AddRange(BitsOf(x));
            bytes.AddRange(BitsOf(y));
            return bytes.ToArray();
        }

        /// <summary>
        /// The WKB of a 2D line string of two points in big-endian byte order, including its own byte order marker.
        /// </summary>
        private static byte[] BigEndianLineString()
        {
            var bytes = new List<byte> { 0x00 };
            bytes.AddRange(BigEndianIntOf(2));
            bytes.AddRange(BigEndianIntOf(2));
            bytes.AddRange(BitsOf(0.0));
            bytes.AddRange(BitsOf(0.0));
            bytes.AddRange(BitsOf(1.0));
            bytes.AddRange(BitsOf(1.0));
            return bytes.ToArray();
        }

        /// <summary>
        /// The WKB of a collection of the given type code holding the one given part, in big-endian byte order.
        /// </summary>
        private static byte[] CollectionOf(int typeCode, byte[] part)
        {
            var bytes = new List<byte> { 0x00 };
            bytes.AddRange(BigEndianIntOf(typeCode));
            bytes.AddRange(BigEndianIntOf(1));
            bytes.AddRange(part);
            return bytes.ToArray();
        }

        /// <summary>
        /// The WKB of a 2D point in little-endian byte order, including its own byte order marker.
        /// </summary>
        private static byte[] LittleEndianPoint(double x, double y)
        {
            var bytes = new List<byte> { 0x01 };
            bytes.AddRange(LittleEndianIntOf(1));
            bytes.AddRange(LittleEndianBitsOf(x));
            bytes.AddRange(LittleEndianBitsOf(y));
            return bytes.ToArray();
        }
    }
}
