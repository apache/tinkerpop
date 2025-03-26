/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphbinary.GraphBinaryCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractTypedCompatibilityTest extends AbstractCompatibilityTest {

    @Test
    public void shouldReadWritePositiveBigDecimal() throws Exception {
        final String resourceName = "pos-bigdecimal";

        final BigDecimal resource = findModelEntryObject(resourceName);
        final BigDecimal fromStatic = read(readFromResource(resourceName), BigDecimal.class);
        final BigDecimal recycled = read(write(fromStatic, BigDecimal.class, resourceName), BigDecimal.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeBigDecimal() throws Exception {
        final String resourceName = "neg-bigdecimal";

        final BigDecimal resource = findModelEntryObject(resourceName);
        final BigDecimal fromStatic = read(readFromResource(resourceName), BigDecimal.class);
        final BigDecimal recycled = read(write(fromStatic, BigDecimal.class, resourceName), BigDecimal.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePositiveBigInteger() throws Exception {
        final String resourceName = "pos-biginteger";

        final BigInteger resource = findModelEntryObject(resourceName);
        final BigInteger fromStatic = read(readFromResource(resourceName), BigInteger.class);
        final BigInteger recycled = read(write(fromStatic, BigInteger.class, resourceName), BigInteger.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeBigInteger() throws Exception {
        final String resourceName = "neg-biginteger";

        final BigInteger resource = findModelEntryObject(resourceName);
        final BigInteger fromStatic = read(readFromResource(resourceName), BigInteger.class);
        final BigInteger recycled = read(write(fromStatic, BigInteger.class, resourceName), BigInteger.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinByte() throws Exception {
        final String resourceName = "min-byte";

        final Byte resource = findModelEntryObject(resourceName);
        final Byte fromStatic = read(readFromResource(resourceName), Byte.class);
        final Byte recycled = read(write(fromStatic, Byte.class, resourceName), Byte.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxByte() throws Exception {
        final String resourceName = "max-byte";

        final Byte resource = findModelEntryObject(resourceName);
        final Byte fromStatic = read(readFromResource(resourceName), Byte.class);
        final Byte recycled = read(write(fromStatic, Byte.class, resourceName), Byte.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEmptyByteBuffer() throws Exception {
        final String resourceName = "empty-binary";

        final ByteBuffer resource = findModelEntryObject(resourceName);
        final ByteBuffer fromStatic = read(readFromResource(resourceName), ByteBuffer.class);
        // have to re-read because once the bytebuffer is flushed to array it will be emptied
        final ByteBuffer recycled = read(write(read(readFromResource(resourceName), ByteBuffer.class), ByteBuffer.class, resourceName), ByteBuffer.class);
        assertNotSame(fromStatic, recycled);
        final byte[] resourceArray = resource.array();
        final byte[] fromStaticArray = fromStatic.array();
        final byte[] recycledArray = recycled.array();
        assertThat(Arrays.equals(fromStaticArray, recycledArray), is(true));
        assertThat(Arrays.equals(resourceArray, fromStaticArray), is(true));
        assertThat(Arrays.equals(resourceArray, recycledArray), is(true));
    }

    @Test
    public void shouldReadWriteStringByteBuffer() throws Exception {
        final String resourceName = "str-binary";

        final ByteBuffer resource = findModelEntryObject(resourceName);
        final ByteBuffer fromStatic = read(readFromResource(resourceName), ByteBuffer.class);
        // have to re-read because once the bytebuffer is flushed to array it will be emptied
        final ByteBuffer recycled = read(write(read(readFromResource(resourceName), ByteBuffer.class), ByteBuffer.class, resourceName), ByteBuffer.class);
        assertNotSame(fromStatic, recycled);
        final byte[] resourceArray = resource.array();
        final byte[] fromStaticArray = fromStatic.array();
        final byte[] recycledArray = recycled.array();
        assertThat(Arrays.equals(fromStaticArray, recycledArray), is(true));
        assertThat(Arrays.equals(resourceArray, fromStaticArray), is(true));
        assertThat(Arrays.equals(resourceArray, recycledArray), is(true));
    }

    @Test
    public void shouldReadWriteMaxDouble() throws Exception {
        final String resourceName = "max-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinDouble() throws Exception {
        final String resourceName = "min-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxNegativeDouble() throws Exception {
        final String resourceName = "neg-max-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinNegativeDouble() throws Exception {
        final String resourceName = "neg-min-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNanDouble() throws Exception {
        final String resourceName = "nan-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePositiveInfinityDouble() throws Exception {
        final String resourceName = "pos-inf-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeInfinityDouble() throws Exception {
        final String resourceName = "neg-inf-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeZeroDouble() throws Exception {
        final String resourceName = "neg-zero-double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteZeroDuration() throws Exception {
        final String resourceName = "zero-duration";

        final Duration resource = findModelEntryObject(resourceName);
        final Duration fromStatic = read(readFromResource(resourceName), Duration.class);
        final Duration recycled = read(write(fromStatic, Duration.class, resourceName), Duration.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteForeverDuration() throws Exception {
        final String resourceName = "forever-duration";

        final Duration resource = findModelEntryObject(resourceName);
        final Duration fromStatic = read(readFromResource(resourceName), Duration.class);
        final Duration recycled = read(write(fromStatic, Duration.class, resourceName), Duration.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEdge() throws Exception {
        final String resourceName = "traversal-edge";

        final Edge resource = findModelEntryObject(resourceName);
        final Edge fromStatic = read(readFromResource(resourceName), Edge.class);
        final Edge recycled = read(write(fromStatic, Edge.class, resourceName), Edge.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertEdge(resource, fromStatic);
        assertEdge(resource, recycled);
    }

    @Test
    public void shouldReadWriteNoPropertyEdge() throws Exception {
        final String resourceName = "no-prop-edge";

        final Edge resource = findModelEntryObject(resourceName);
        final Edge fromStatic = read(readFromResource(resourceName), Edge.class);
        final Edge recycled = read(write(fromStatic, Edge.class, resourceName), Edge.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertEdge(resource, fromStatic);
        assertEdge(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxFloat() throws Exception {
        final String resourceName = "max-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinFloat() throws Exception {
        final String resourceName = "min-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxNegativeFloat() throws Exception {
        final String resourceName = "neg-max-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinNegativeFloat() throws Exception {
        final String resourceName = "neg-min-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNanFloat() throws Exception {
        final String resourceName = "nan-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePositiveInfinityFloat() throws Exception {
        final String resourceName = "pos-inf-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeInfinityFloat() throws Exception {
        final String resourceName = "neg-inf-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNegativeZeroFloat() throws Exception {
        final String resourceName = "neg-zero-float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxInteger() throws Exception {
        final String resourceName = "max-int";

        final Integer resource = findModelEntryObject(resourceName);
        final Integer fromStatic = read(readFromResource(resourceName), Integer.class);
        final Integer recycled = read(write(fromStatic, Integer.class, resourceName), Integer.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinInteger() throws Exception {
        final String resourceName = "min-int";

        final Integer resource = findModelEntryObject(resourceName);
        final Integer fromStatic = read(readFromResource(resourceName), Integer.class);
        final Integer recycled = read(write(fromStatic, Integer.class, resourceName), Integer.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxLong() throws Exception {
        final String resourceName = "max-long";

        final Long resource = findModelEntryObject(resourceName);
        final Long fromStatic = read(readFromResource(resourceName), Long.class);
        final Long recycled = read(write(fromStatic, Long.class, resourceName), Long.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinLong() throws Exception {
        final String resourceName = "min-long";

        final Long resource = findModelEntryObject(resourceName);
        final Long fromStatic = read(readFromResource(resourceName), Long.class);
        final Long recycled = read(write(fromStatic, Long.class, resourceName), Long.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMultiTypeList() throws Exception {
        final String resourceName = "var-type-list";

        final List resource = findModelEntryObject(resourceName);
        final List fromStatic = read(readFromResource(resourceName), List.class);
        final List recycled = read(write(fromStatic, List.class, resourceName), List.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEmptyList() throws Exception {
        final String resourceName = "empty-list";

        final List resource = findModelEntryObject(resourceName);
        final List fromStatic = read(readFromResource(resourceName), List.class);
        final List recycled = read(write(fromStatic, List.class, resourceName), List.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMultiTypeMap() throws Exception {
        final String resourceName = "var-type-map";

        final Map resource = findModelEntryObject(resourceName);
        final Map fromStatic = read(readFromResource(resourceName), Map.class);
        final Map recycled = read(write(fromStatic, Map.class, resourceName), Map.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEmptyMap() throws Exception {
        final String resourceName = "empty-map";

        final Map resource = findModelEntryObject(resourceName);
        final Map fromStatic = read(readFromResource(resourceName), Map.class);
        final Map recycled = read(write(fromStatic, Map.class, resourceName), Map.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxOffsetDateTime() throws Exception {
        final String resourceName = "max-offsetdatetime";

        final OffsetDateTime resource = findModelEntryObject(resourceName);
        final OffsetDateTime fromStatic = read(readFromResource(resourceName), OffsetDateTime.class);
        final OffsetDateTime recycled = read(write(fromStatic, OffsetDateTime.class, resourceName), OffsetDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinOffsetDateTime() throws Exception {
        final String resourceName = "min-offsetdatetime";

        final OffsetDateTime resource = findModelEntryObject(resourceName);
        final OffsetDateTime fromStatic = read(readFromResource(resourceName), OffsetDateTime.class);
        final OffsetDateTime recycled = read(write(fromStatic, OffsetDateTime.class, resourceName), OffsetDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePath() throws Exception {
        final String resourceName = "traversal-path";

        final Path resource = findModelEntryObject(resourceName);
        final Path fromStatic = read(readFromResource(resourceName), Path.class);
        final Path recycled = read(write(fromStatic, Path.class, resourceName), Path.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEmptyPath() throws Exception {
        final String resourceName = "empty-path";

        final Path resource = findModelEntryObject(resourceName);
        final Path fromStatic = read(readFromResource(resourceName), Path.class);
        final Path recycled = read(write(fromStatic, Path.class, resourceName), Path.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(fromStatic, resource);
        assertEquals(recycled, resource);
    }

    @Test
    public void shouldReadWritePathWithProperties() throws Exception {
        final String resourceName = "prop-path";

        final Path resource = findModelEntryObject(resourceName);
        final Path fromStatic = read(readFromResource(resourceName), Path.class);
        final Path recycled = read(write(fromStatic, Path.class, resourceName), Path.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEdgeProperty() throws Exception {
        final String resourceName = "edge-property";

        final Property resource = findModelEntryObject(resourceName);
        final Property fromStatic = read(readFromResource(resourceName), Property.class);
        final Property recycled = read(write(fromStatic, Property.class, resourceName), Property.class);
        assertNotSame(fromStatic, recycled);
        assertProperty(resource, fromStatic);
        assertProperty(resource, recycled);
    }

    @Test
    public void shouldReadWriteNullProperty() throws Exception {
        final String resourceName = "null-property";

        final Property resource = findModelEntryObject(resourceName);
        final Property fromStatic = read(readFromResource(resourceName), Property.class);
        final Property recycled = read(write(fromStatic, Property.class, resourceName), Property.class);
        assertNotSame(fromStatic, recycled);
        assertProperty(resource, fromStatic);
        assertProperty(resource, recycled);
    }

    @Test
    public void shouldReadWriteMultiTypeSet() throws Exception {
        final String resourceName = "var-type-set";

        final Set resource = findModelEntryObject(resourceName);
        final Set fromStatic = read(readFromResource(resourceName), Set.class);
        final Set recycled = read(write(fromStatic, Set.class, resourceName), Set.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEmptySet() throws Exception {
        final String resourceName = "empty-set";

        final Set resource = findModelEntryObject(resourceName);
        final Set fromStatic = read(readFromResource(resourceName), Set.class);
        final Set recycled = read(write(fromStatic, Set.class, resourceName), Set.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMaxShort() throws Exception {
        final String resourceName = "max-short";

        final Short resource = findModelEntryObject(resourceName);
        final Short fromStatic = read(readFromResource(resourceName), Short.class);
        final Short recycled = read(write(fromStatic, Short.class, resourceName), Short.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMinShort() throws Exception {
        final String resourceName = "min-short";

        final Short resource = findModelEntryObject(resourceName);
        final Short fromStatic = read(readFromResource(resourceName), Short.class);
        final Short recycled = read(write(fromStatic, Short.class, resourceName), Short.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTinkerGraph() throws Exception {
        final String resourceName = "tinker-graph";

        final TinkerGraph fromStatic = read(readFromResource(resourceName), TinkerGraph.class);
        final TinkerGraph recycled = read(write(fromStatic, TinkerGraph.class, resourceName), TinkerGraph.class);
        assertNotSame(fromStatic, recycled);

        IoTest.assertCrewGraph(fromStatic, false);
        IoTest.assertCrewGraph(recycled, false);
    }

    @Test
    public void shouldReadWriteUUID() throws Exception {
        final String resourceName = "specified-uuid";

        final UUID resource = findModelEntryObject(resourceName);
        final UUID fromStatic = read(readFromResource(resourceName), UUID.class);
        final UUID recycled = read(write(fromStatic, UUID.class, resourceName), UUID.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNilUUID() throws Exception {
        final String resourceName = "nil-uuid";

        final UUID resource = findModelEntryObject(resourceName);
        final UUID fromStatic = read(readFromResource(resourceName), UUID.class);
        final UUID recycled = read(write(fromStatic, UUID.class, resourceName), UUID.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteVertex() throws Exception {
        final String resourceName = "traversal-vertex";

        final Vertex resource = findModelEntryObject(resourceName);
        final Vertex fromStatic = read(readFromResource(resourceName), Vertex.class);
        final Vertex recycled = read(write(fromStatic, Vertex.class, resourceName), Vertex.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertVertex(resource, fromStatic);
        assertVertex(resource, recycled);
    }

    @Test
    public void shouldReadWriteNoPropertyVertex() throws Exception {
        final String resourceName = "no-prop-vertex";

        final Vertex resource = findModelEntryObject(resourceName);
        final Vertex fromStatic = read(readFromResource(resourceName), Vertex.class);
        final Vertex recycled = read(write(fromStatic, Vertex.class, resourceName), Vertex.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertVertex(resource, fromStatic);
        assertVertex(resource, recycled);
    }

    @Test
    public void shouldReadWriteVertexProperty() throws Exception {
        final String resourceName = "traversal-vertexproperty";

        final VertexProperty resource = findModelEntryObject(resourceName);
        final VertexProperty fromStatic = read(readFromResource(resourceName), VertexProperty.class);
        final VertexProperty recycled = read(write(fromStatic, VertexProperty.class, resourceName), VertexProperty.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertVertexProperty(resource, recycled);
        assertVertexProperty(resource, fromStatic);
    }

    @Test
    public void shouldReadWriteMetapropertyVertexProperty() throws Exception {
        final String resourceName = "meta-vertexproperty";

        final VertexProperty resource = findModelEntryObject(resourceName);
        final VertexProperty fromStatic = read(readFromResource(resourceName), VertexProperty.class);
        final VertexProperty recycled = read(write(fromStatic, VertexProperty.class, resourceName), VertexProperty.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertVertexProperty(resource, recycled);
        assertVertexProperty(resource, fromStatic);
    }

    @Test
    public void shouldReadWriteSetVertexProperty() throws Exception {
        final String resourceName = "set-cardinality-vertexproperty";

        final VertexProperty resource = findModelEntryObject(resourceName);
        final VertexProperty fromStatic = read(readFromResource(resourceName), VertexProperty.class);
        final VertexProperty recycled = read(write(fromStatic, VertexProperty.class, resourceName), VertexProperty.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertVertexProperty(resource, recycled);
        assertVertexProperty(resource, fromStatic);
    }

    @Test
    public void shouldReadWriteIdT() throws Exception {
        final String resourceName = "id-t";

        final T resource = findModelEntryObject(resourceName);
        final T fromStatic = read(readFromResource(resourceName), T.class);
        final T recycled = read(write(fromStatic, T.class, resourceName), T.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOutDirection() throws Exception {
        final String resourceName = "out-direction";

        final Direction resource = findModelEntryObject(resourceName);
        final Direction fromStatic = read(readFromResource(resourceName), Direction.class);
        final Direction recycled = read(write(fromStatic, Direction.class, resourceName), Direction.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteVarBulkList() throws Exception {
        final String resourceName = "var-bulklist";

        final BulkSet resource = findModelEntryObject(resourceName);
        final List fromStatic = read(readFromResource(resourceName), List.class);
        final List recycled = read(write(fromStatic, BulkSet.class, resourceName), List.class);
        assertEquals(fromStatic, recycled);
        // we no longer deserialize into BulkSets, needs the expanded list
        List expandedResource = new ArrayList<>();
        resource.spliterator().forEachRemaining(expandedResource::add);
        assertEquals(expandedResource, fromStatic);
        assertEquals(expandedResource, recycled);
    }

    @Test
    public void shouldReadWriteEmptyBulkList() throws Exception {
        final String resourceName = "empty-bulklist";

        final BulkSet resource = findModelEntryObject(resourceName);
        final List fromStatic = read(readFromResource(resourceName), List.class);
        final List recycled = read(write(fromStatic, BulkSet.class, resourceName), List.class);
        assertEquals(fromStatic, recycled);
        // we no longer deserialize into BulkSets, needs the expanded list
        List expandedResource = new ArrayList<>();
        resource.spliterator().forEachRemaining(expandedResource::add);
        assertEquals(expandedResource, fromStatic);
        assertEquals(expandedResource, recycled);
    }

    @Test
    public void shouldReadWriteSingleByteChar() throws Exception {
        final String resourceName = "single-byte-char";

        final Character resource = findModelEntryObject(resourceName);
        final Character fromStatic = read(readFromResource(resourceName), Character.class);
        final Character recycled = read(write(fromStatic, Character.class, resourceName), Character.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMultiByteChar() throws Exception {
        final String resourceName = "multi-byte-char";

        final Character resource = findModelEntryObject(resourceName);
        final Character fromStatic = read(readFromResource(resourceName), Character.class);
        final Character recycled = read(write(fromStatic, Character.class, resourceName), Character.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteNull() throws Exception {
        final String resourceName = "unspecified-null";

        final Character resource = findModelEntryObject(resourceName);
        final Character fromStatic = read(readFromResource(resourceName), Character.class);
        final Character recycled = read(write(fromStatic, Character.class, resourceName), Character.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTrueBoolean() throws Exception {
        final String resourceName = "true-boolean";

        final Boolean resource = findModelEntryObject(resourceName);
        final Boolean fromStatic = read(readFromResource(resourceName), Boolean.class);
        final Boolean recycled = read(write(fromStatic, Boolean.class, resourceName), Boolean.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteFalseBoolean() throws Exception {
        final String resourceName = "false-boolean";

        final Boolean resource = findModelEntryObject(resourceName);
        final Boolean fromStatic = read(readFromResource(resourceName), Boolean.class);
        final Boolean recycled = read(write(fromStatic, Boolean.class, resourceName), Boolean.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteSingleByteString() throws Exception {
        final String resourceName = "single-byte-string";

        final String resource = findModelEntryObject(resourceName);
        final String fromStatic = read(readFromResource(resourceName), String.class);
        final String recycled = read(write(fromStatic, String.class, resourceName), String.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMixedString() throws Exception {
        final String resourceName = "mixed-string";

        final String resource = findModelEntryObject(resourceName);
        final String fromStatic = read(readFromResource(resourceName), String.class);
        final String recycled = read(write(fromStatic, String.class, resourceName), String.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTraversalTree() throws Exception {
        final String resourceName = "traversal-tree";

        final Tree resource = findModelEntryObject(resourceName);
        final Tree fromStatic = read(readFromResource(resourceName), Tree.class);
        final Tree recycled = read(write(fromStatic, Tree.class, resourceName), Tree.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }
}
