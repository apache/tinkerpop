/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser.binary.types;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.GeometryCollection;
import org.apache.tinkerpop.gremlin.structure.geo.LineString;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.geo.MultiLineString;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPoint;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPolygon;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.geo.Polygon;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.GeometrySerializer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GeometrySerializerTest {

    private static final GraphBinaryReader reader = new GraphBinaryReader();
    private static final GraphBinaryWriter writer = new GraphBinaryWriter();
    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    /**
     * The value of {@code POINT(-122.3321 47.6062)} with SRID 4326: the SRID followed by the
     * big-endian ISO WKB, with no length before it.
     */
    private static final byte[] POINT_VALUE = new byte[]{
            0x00, 0x00, 0x10, (byte) 0xe6, // SRID 4326
            0x00, // WKB big-endian
            0x00, 0x00, 0x00, 0x01, // WKB Point
            (byte) 0xc0, 0x5e, (byte) 0x95, 0x41, 0x20, 0x5b, (byte) 0xc0, 0x1a, // x = -122.3321
            0x40, 0x47, (byte) 0xcd, (byte) 0x97, (byte) 0xf6, 0x2b, 0x6a, (byte) 0xe8 // y = 47.6062
    };

    /**
     * The WKB of the same point written little-endian, which a reader accepts and a writer
     * never produces.
     */
    private static final byte[] LITTLE_ENDIAN_POINT_WKB = new byte[]{
            0x01, // WKB little-endian
            0x01, 0x00, 0x00, 0x00, // WKB Point
            0x1a, (byte) 0xc0, 0x5b, 0x20, 0x41, (byte) 0x95, 0x5e, (byte) 0xc0, // x = -122.3321
            (byte) 0xe8, 0x6a, 0x2b, (byte) 0xf6, (byte) 0x97, (byte) 0xcd, 0x47, 0x40 // y = 47.6062
    };

    private static final byte[] NAN_BYTES = new byte[]{0x7f, (byte) 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    private static final Point GEOGRAPHIC_POINT = Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC);

    @Test
    public void shouldReadValueFromSridAndWkb() throws IOException {
        final Geometry actual = reader.readValue(bufferOf(POINT_VALUE), Geometry.class, false);

        assertEquals(GEOGRAPHIC_POINT, actual);
        assertEquals(Model.GEOGRAPHIC, actual.model());
    }

    @Test
    public void shouldWriteFullyQualifiedValueWithTypeCode() throws IOException {
        final Buffer buffer = buffer();
        writer.write(GEOGRAPHIC_POINT, buffer);

        assertArrayEquals(concat(new byte[]{0x31, 0x00}, POINT_VALUE), bytesOf(buffer));
    }

    @Test
    public void shouldWriteNullAsTypeCodeAndNullFlag() throws IOException {
        final Buffer buffer = buffer();
        // GraphBinaryWriter.write(null, ...) writes an unspecified null (fe 01), so the typed
        // null is composed directly from the type code and the serializer's null value flag.
        buffer.writeByte(DataType.GEOMETRY.getCodeByte());
        new GeometrySerializer().write(null, buffer, writer);

        assertArrayEquals(new byte[]{0x31, 0x01}, bytesOf(buffer));
    }

    @Test
    public void shouldReadNullFromTypeCodeAndNullFlag() throws IOException {
        assertNull(reader.read(bufferOf(new byte[]{0x31, 0x01})));
    }

    @Test
    public void shouldFailToWriteNullWhenNotNullable() {
        final Buffer buffer = buffer();

        assertThrows(IOException.class, () -> new GeometrySerializer().writeValue(null, buffer, writer, false));
    }

    @Test
    public void shouldWriteAnEmptyPointAsNaNCoordinates() throws IOException {
        final Buffer buffer = buffer();
        writer.writeValue(Point.empty(), buffer, false);

        assertArrayEquals(concat(
                new byte[]{0x00, 0x00, 0x00, 0x00}, // SRID 0
                new byte[]{0x00, 0x00, 0x00, 0x00, 0x01}, // big-endian WKB Point
                NAN_BYTES, NAN_BYTES), bytesOf(buffer));

        final Geometry read = writeAndRead(Point.empty());
        assertEquals(Point.empty(), read);
        assertTrue(read.isEmpty());
    }

    @Test
    public void shouldWriteAPointWithZAsTheZTypeCode() throws IOException {
        final Point point = Point.of(1, 2, 3);
        final Buffer buffer = buffer();
        writer.writeValue(point, buffer, false);

        // the SRID and the byte order marker come first, then the Point Z type code 1001
        final byte[] bytes = bytesOf(buffer);
        assertArrayEquals(new byte[]{0x00, 0x00, 0x03, (byte) 0xe9}, Arrays.copyOfRange(bytes, 5, 9));

        final Geometry read = writeAndRead(point);
        assertEquals(point, read);
        assertTrue(read.hasZ());
    }

    @Test
    public void shouldRoundTripEveryKind() throws IOException {
        final String[] values = {
                "POINT(1.5 -2.5)", "POINT Z (1.5 -2.5 3.5)",
                "LINESTRING(0 0, 1 1, 2 -2.5)",
                "POLYGON((0 0, 10 0, 10 10, 0 0), (1 1, 2 1, 2 2, 1 1))",
                "MULTIPOINT((0 0), (1 1))",
                "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
                "GEOMETRYCOLLECTION(POINT EMPTY, GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)))"
        };

        for (final String wkt : values) {
            final Geometry geometry = Geometry.fromWkt(wkt);
            final Geometry read = writeAndRead(geometry);

            assertEquals(wkt, geometry, read);
            assertEquals(wkt, geometry.getClass(), read.getClass());
        }
    }

    @Test
    public void shouldRoundTripEveryEmptyKind() throws IOException {
        final String[] values = {
                "POINT EMPTY", "POINT Z EMPTY",
                "LINESTRING EMPTY", "LINESTRING Z EMPTY",
                "POLYGON EMPTY", "POLYGON Z EMPTY",
                "MULTIPOINT EMPTY", "MULTIPOINT Z EMPTY",
                "MULTILINESTRING EMPTY", "MULTILINESTRING Z EMPTY",
                "MULTIPOLYGON EMPTY", "MULTIPOLYGON Z EMPTY",
                "GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION Z EMPTY"
        };

        for (final String wkt : values) {
            final Geometry geometry = Geometry.fromWkt(wkt);
            final Geometry read = writeAndRead(geometry);

            assertEquals(wkt, geometry, read);
            assertEquals(wkt, geometry.getClass(), read.getClass());
            assertTrue(wkt, read.isEmpty());
            assertEquals(wkt, geometry.hasZ(), read.hasZ());
        }
    }

    @Test
    public void shouldRoundTripAPointWithACustomSrid() throws IOException {
        final Geometry read = writeAndRead(Geometry.fromWkt("POINT(1 2)", 3857));

        assertEquals(3857, read.srid());
        assertEquals(Model.OTHER, read.model());
    }

    @Test
    public void shouldKeepNegativeZeroOnRoundTrip() throws IOException {
        final Point read = (Point) writeAndRead(Point.of(-0.0, 0.0));

        assertEquals(Long.MIN_VALUE, Double.doubleToRawLongBits(read.x()));
        assertEquals(Point.of(0, 0), read);
    }

    @Test
    public void shouldReadLittleEndianWkbAndWriteItBackBigEndian() throws IOException {
        final byte[] value = concat(new byte[]{0x00, 0x00, 0x10, (byte) 0xe6}, LITTLE_ENDIAN_POINT_WKB);
        final Geometry read = reader.readValue(bufferOf(value), Geometry.class, false);

        assertEquals(GEOGRAPHIC_POINT, read);

        final Buffer buffer = buffer();
        writer.writeValue(read, buffer, false);
        assertArrayEquals(POINT_VALUE, bytesOf(buffer));
    }

    @Test
    public void shouldReadAGeometryFollowedByAnotherValue() throws IOException {
        final List<Object> values = Arrays.asList(Point.of(1, 2), 42);
        final Buffer buffer = buffer();
        writer.write(values, buffer);

        final List<Object> read = reader.read(buffer);

        assertEquals(values, read);
        assertEquals(0, buffer.readableBytes());
    }

    @Test
    public void shouldReadGeometriesOfDifferentKindsInOrder() throws IOException {
        final List<Geometry> values = Arrays.asList(
                Point.of(1, 2),
                LineString.of(Point.of(0, 0), Point.of(1, 1)),
                MultiPoint.of(Point.of(2, 2), Point.empty()));
        final Buffer buffer = buffer();
        writer.write(values, buffer);

        final List<Geometry> read = reader.read(buffer);

        assertEquals(values, read);
        assertEquals(0, buffer.readableBytes());
    }

    @Test
    public void shouldFailWhenTheValueEndsInsideTheWkb() {
        // a value cut inside the y ordinate of a point
        assertEquals("The geometry value ends before its WKB is complete",
                assertFailsToRead(Arrays.copyOf(POINT_VALUE, POINT_VALUE.length - 3)).getMessage());

        // a value holding an SRID and no WKB bytes at all
        assertEquals("The geometry value ends before its WKB is complete",
                assertFailsToRead(intBytes(4326)).getMessage());

        // a MultiPoint declaring two points and holding one, so the value ends on the byte
        // order marker the second point would begin with
        final byte[] oneOfTwoPoints = concat(intBytes(0), header(4, 2),
                new byte[]{0x00}, intBytes(1), doubleBytes(0.0), doubleBytes(0.0));
        assertEquals(34, oneOfTwoPoints.length);
        assertEquals("The geometry value ends before its WKB is complete",
                assertFailsToRead(oneOfTwoPoints).getMessage());
    }

    @Test
    public void shouldFailOnAnOversizedPointCount() {
        // a line string declaring Integer.MAX_VALUE points, holding one. A count is never
        // sized up front, so the value ends while the declared points are being read.
        final byte[] value = concat(intBytes(0), header(2, 0x7FFFFFFF), doubleBytes(0.0), doubleBytes(0.0));
        assertEquals(29, value.length);

        assertEquals("The geometry value ends before its WKB is complete",
                assertFailsToRead(value).getMessage());
    }

    @Test
    public void shouldFailOnEwkb() {
        final IOException ex = assertFailsToRead(concat(intBytes(4326),
                new byte[]{0x00, 0x20, 0x00, 0x00, 0x01}, // byte order and the EWKB SRID flag
                new byte[]{0x00, 0x00, 0x10, (byte) 0xe6})); // the SRID the EWKB carries

        assertTrue("Message should mention EWKB, was: " + ex.getMessage(), ex.getMessage().contains("EWKB"));
    }

    @Test
    public void shouldFailOnMAndZmTypeCodes() {
        final IOException m = assertFailsToRead(concat(intBytes(0), new byte[]{0x00}, intBytes(2001)));
        assertTrue("Message should mention the M dimension, was: " + m.getMessage(),
                m.getMessage().contains("M dimension not supported"));

        final IOException zm = assertFailsToRead(concat(intBytes(0), new byte[]{0x00}, intBytes(3001)));
        assertTrue("Message should mention the ZM dimension, was: " + zm.getMessage(),
                zm.getMessage().contains("ZM dimension not supported"));
    }

    @Test
    public void shouldFailOnAnUnknownTypeCode() {
        final IOException ex = assertFailsToRead(concat(intBytes(0), new byte[]{0x00}, intBytes(8)));

        assertTrue("Message should mention an unknown type code, was: " + ex.getMessage(),
                ex.getMessage().contains("Unknown WKB type code"));
    }

    @Test
    public void shouldFailOnANaNVertexInALineString() {
        final IOException ex = assertFailsToRead(concat(intBytes(0), header(2, 2),
                NAN_BYTES, NAN_BYTES,
                doubleBytes(1.0), doubleBytes(1.0)));

        assertEquals("A linestring vertex cannot be an empty point; a vertex must be a real coordinate",
                ex.getMessage());
    }

    @Test
    public void shouldFailOnAGeographicCoordinateOutOfRange() {
        // a point whose longitude is past 180 degrees, under SRID 4326
        final byte[] value = concat(intBytes(4326), new byte[]{0x00}, intBytes(1),
                doubleBytes(181.0), doubleBytes(0.0));
        assertEquals(25, value.length);

        assertEquals("GEOGRAPHIC longitude must be in [-180, 180], got: 181.0",
                assertFailsToRead(value).getMessage());
    }

    @Test
    public void shouldFailOnNestingDeeperThan32Levels() {
        final byte[] value = concat(intBytes(0), nestedCollections(33));
        assertEquals(322, value.length);

        assertEquals("Collection nesting depth exceeds maximum of 32",
                assertFailsToRead(value).getMessage());
    }

    @Test
    public void shouldResolveTheGeometrySerializerForEveryKind() throws IOException {
        for (final Class<? extends Geometry> kind : Arrays.asList(Point.class, LineString.class, Polygon.class,
                MultiPoint.class, MultiLineString.class, MultiPolygon.class, GeometryCollection.class)) {
            assertTrue(kind.getSimpleName(),
                    TypeSerializerRegistry.INSTANCE.getSerializer(kind) instanceof GeometrySerializer);
        }
    }

    private static Buffer buffer() {
        return bufferFactory.create(allocator.buffer());
    }

    private static Buffer bufferOf(final byte[] bytes) {
        return bufferFactory.create(Unpooled.wrappedBuffer(bytes));
    }

    private static byte[] bytesOf(final Buffer buffer) {
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

    private static Geometry writeAndRead(final Geometry value) throws IOException {
        final Buffer buffer = buffer();
        writer.write(value, buffer);
        return reader.read(buffer);
    }

    /**
     * Reads the given value bytes as a geometry and returns the failure it raises.
     */
    private static IOException assertFailsToRead(final byte[] value) {
        final Buffer buffer = bufferOf(value);
        return assertThrows(IOException.class, () -> reader.readValue(buffer, Geometry.class, false));
    }

    /**
     * Returns a big-endian geometry header followed by one count.
     */
    private static byte[] header(final int typeCode, final int count) {
        return concat(new byte[]{0x00}, intBytes(typeCode), intBytes(count));
    }

    /**
     * Returns the WKB of the given number of GeometryCollection levels wrapping a point.
     */
    private static byte[] nestedCollections(final int levels) {
        final ByteBuffer bb = ByteBuffer.allocate(levels * 9 + 21).order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < levels; i++) {
            bb.put((byte) 0x00);
            bb.putInt(7);
            bb.putInt(1);
        }
        bb.put((byte) 0x00);
        bb.putInt(1);
        bb.putDouble(0.0);
        bb.putDouble(0.0);
        return bb.array();
    }

    private static byte[] intBytes(final int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static byte[] doubleBytes(final double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array();
    }

    private static byte[] concat(final byte[]... parts) {
        int length = 0;
        for (final byte[] part : parts) {
            length += part.length;
        }
        final ByteBuffer bb = ByteBuffer.allocate(length);
        for (final byte[] part : parts) {
            bb.put(part);
        }
        return bb.array();
    }
}
