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
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
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
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
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
    public void shouldReadWriteBarrier() throws Exception {
        final String resourceName = "barrier";

        final SackFunctions.Barrier resource = findModelEntryObject(resourceName);
        final SackFunctions.Barrier fromStatic = read(readFromResource(resourceName), SackFunctions.Barrier.class);
        final SackFunctions.Barrier recycled = read(write(fromStatic, SackFunctions.Barrier.class, resourceName), SackFunctions.Barrier.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBigDecimal() throws Exception {
        final String resourceName = "bigdecimal";

        final BigDecimal resource = findModelEntryObject(resourceName);
        final BigDecimal fromStatic = read(readFromResource(resourceName), BigDecimal.class);
        final BigDecimal recycled = read(write(fromStatic, BigDecimal.class, resourceName), BigDecimal.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBigInteger() throws Exception {
        final String resourceName = "biginteger";

        final BigInteger resource = findModelEntryObject(resourceName);
        final BigInteger fromStatic = read(readFromResource(resourceName), BigInteger.class);
        final BigInteger recycled = read(write(fromStatic, BigInteger.class, resourceName), BigInteger.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBinding() throws Exception {
        final String resourceName = "binding";

        final GremlinLang.Binding resource = findModelEntryObject(resourceName);
        final GremlinLang.Binding fromStatic = read(readFromResource(resourceName), GremlinLang.Binding.class);
        final GremlinLang.Binding recycled = read(write(fromStatic, GremlinLang.Binding.class, resourceName), GremlinLang.Binding.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteByte() throws Exception {
        final String resourceName = "byte";

        final Byte resource = findModelEntryObject(resourceName);
        final Byte fromStatic = read(readFromResource(resourceName), Byte.class);
        final Byte recycled = read(write(fromStatic, Byte.class, resourceName), Byte.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteByteBuffer() throws Exception {
        final String resourceName = "bytebuffer";

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
    public void shouldReadWriteBytecode() throws Exception {
        final String resourceName = "bytecode";

        final GremlinLang fromStatic = read(readFromResource(resourceName), GremlinLang.class);
        final GremlinLang recycled = read(write(fromStatic, GremlinLang.class, resourceName), GremlinLang.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        // can't reasonably assert the bytecode against the original model as changes to strategies over time might
        // alter the bytecode form and then break the test. the assertions as they are ensure that the core of
        // serialization is correct by ensuring the contents of bytecode (whether they are valid for a specific version
        // or not). it seems beyond the scope of these tests to validate that bytecode itself is unchanging and fully
        // backward compatible (at least for now).
    }

    @Test
    public void shouldReadWriteCardinality() throws Exception {
        final String resourceName = "cardinality";

        final VertexProperty.Cardinality resource = findModelEntryObject(resourceName);
        final VertexProperty.Cardinality fromStatic = read(readFromResource(resourceName), VertexProperty.Cardinality.class);
        final VertexProperty.Cardinality recycled = read(write(fromStatic, VertexProperty.Cardinality.class, resourceName), VertexProperty.Cardinality.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteChar() throws Exception {
        final String resourceName = "char";

        final Character resource = findModelEntryObject(resourceName);
        final Character fromStatic = read(readFromResource(resourceName), Character.class);
        final Character recycled = read(write(fromStatic, Character.class, resourceName), Character.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteClass() throws Exception {
        final String resourceName = "class";

        final Class resource = findModelEntryObject(resourceName);
        final Class fromStatic = read(readFromResource(resourceName), Class.class);
        final Class recycled = read(write(fromStatic, Class.class, resourceName), Class.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteColumn() throws Exception {
        final String resourceName = "column";

        final Column resource = findModelEntryObject(resourceName);
        final Column fromStatic = read(readFromResource(resourceName), Column.class);
        final Column recycled = read(write(fromStatic, Column.class, resourceName), Column.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDate() throws Exception {
        final String resourceName = "date";

        final Date resource = findModelEntryObject(resourceName);
        final Date fromStatic = read(readFromResource(resourceName), Date.class);
        final Date recycled = read(write(fromStatic, Date.class, resourceName), Date.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDirection() throws Exception {
        final String resourceName = "direction";

        final Direction resource = findModelEntryObject(resourceName);
        final Direction fromStatic = read(readFromResource(resourceName), Direction.class);
        final Direction recycled = read(write(fromStatic, Direction.class, resourceName), Direction.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDouble() throws Exception {
        final String resourceName = "double";

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class, resourceName), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDuration() throws Exception {
        final String resourceName = "duration";

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
        final String resourceName = "edge";

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
    public void shouldReadWriteFloat() throws Exception {
        final String resourceName = "float";

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class, resourceName), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteINetAddress() throws Exception {
        final String resourceName = "inetaddress";

        final InetAddress resource = findModelEntryObject(resourceName);
        final InetAddress fromStatic = read(readFromResource(resourceName), InetAddress.class);
        final InetAddress recycled = read(write(fromStatic, Float.class, resourceName), InetAddress.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteInstant() throws Exception {
        final String resourceName = "instant";

        final Instant resource = findModelEntryObject(resourceName);
        final Instant fromStatic = read(readFromResource(resourceName), Instant.class);
        final Instant recycled = read(write(fromStatic, Instant.class, resourceName), Instant.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteInteger() throws Exception {
        final String resourceName = "integer";

        final Integer resource = findModelEntryObject(resourceName);
        final Integer fromStatic = read(readFromResource(resourceName), Integer.class);
        final Integer recycled = read(write(fromStatic, Integer.class, resourceName), Integer.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLambda() throws Exception {
        final String resourceName = "lambda";

        final Lambda resource = findModelEntryObject(resourceName);
        final Lambda fromStatic = read(readFromResource(resourceName), Lambda.class);
        final Lambda recycled = read(write(fromStatic, Lambda.class, resourceName), Lambda.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalDate() throws Exception {
        final String resourceName = "localdate";

        final LocalDate resource = findModelEntryObject(resourceName);
        final LocalDate fromStatic = read(readFromResource(resourceName), LocalDate.class);
        final LocalDate recycled = read(write(fromStatic, LocalDate.class, resourceName), LocalDate.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalDateTime() throws Exception {
        final String resourceName = "localdatetime";

        final LocalDateTime resource = findModelEntryObject(resourceName);
        final LocalDateTime fromStatic = read(readFromResource(resourceName), LocalDateTime.class);
        final LocalDateTime recycled = read(write(fromStatic, LocalDateTime.class, resourceName), LocalDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalTime() throws Exception {
        final String resourceName = "localtime";

        final LocalTime resource = findModelEntryObject(resourceName);
        final LocalTime fromStatic = read(readFromResource(resourceName), LocalTime.class);
        final LocalTime recycled = read(write(fromStatic, LocalTime.class, resourceName), LocalTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLong() throws Exception {
        final String resourceName = "long";

        final Long resource = findModelEntryObject(resourceName);
        final Long fromStatic = read(readFromResource(resourceName), Long.class);
        final Long recycled = read(write(fromStatic, Long.class, resourceName), Long.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteList() throws Exception {
        final String resourceName = "list";

        assumeThat("GraphSON v2 didn't explicitly have List as a type",
                is(this.getClass().equals(GraphSONTypedCompatibilityTest.class) && getCompatibility().equals("v2")));

        final List resource = findModelEntryObject(resourceName);
        final List fromStatic = read(readFromResource(resourceName), List.class);
        final List recycled = read(write(fromStatic, List.class, resourceName), List.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMap() throws Exception {
        final String resourceName = "map";

        assumeThat("GraphSON v2 didn't explicitly have Map as a type",
                is(this.getClass().equals(GraphSONTypedCompatibilityTest.class) && getCompatibility().equals("v2")));

        final Map resource = findModelEntryObject(resourceName);
        final Map fromStatic = read(readFromResource(resourceName), Map.class);
        final Map recycled = read(write(fromStatic, Map.class, resourceName), Map.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMetrics() throws Exception {
        final String resourceName = "metrics";

        final Metrics fromStatic = read(readFromResource(resourceName), Metrics.class);
        final Metrics recycled = read(write(fromStatic, Metrics.class, resourceName), Metrics.class);
        assertNotSame(fromStatic, recycled);
        // have to do compares on the object read from resources because it has statically calculated values. the
        // "new" one from Model is generated dynamically from a traversal and thus has variations in properties that
        // are based on time
        assertEquals(fromStatic.getAnnotations(), recycled.getAnnotations());
        assertEquals(fromStatic.getCounts(), recycled.getCounts());
        assertEquals(fromStatic.getDuration(TimeUnit.MILLISECONDS), recycled.getDuration(TimeUnit.MILLISECONDS));
    }

    @Test
    public void shouldReadWriteMonthDay() throws Exception {
        final String resourceName = "monthday";

        final MonthDay resource = findModelEntryObject(resourceName);
        final MonthDay fromStatic = read(readFromResource(resourceName), MonthDay.class);
        final MonthDay recycled = read(write(fromStatic, MonthDay.class, resourceName), MonthDay.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOffsetDateTime() throws Exception {
        final String resourceName = "offsetdatetime";

        final OffsetDateTime resource = findModelEntryObject(resourceName);
        final OffsetDateTime fromStatic = read(readFromResource(resourceName), OffsetDateTime.class);
        final OffsetDateTime recycled = read(write(fromStatic, OffsetDateTime.class, resourceName), OffsetDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOffsetTime() throws Exception {
        final String resourceName = "offsettime";

        final OffsetTime resource = findModelEntryObject(resourceName);
        final OffsetTime fromStatic = read(readFromResource(resourceName), OffsetTime.class);
        final OffsetTime recycled = read(write(fromStatic, OffsetTime.class, resourceName), OffsetTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOperator() throws Exception {
        final String resourceName = "operator";

        final Operator resource = findModelEntryObject(resourceName);
        final Operator fromStatic = read(readFromResource(resourceName), Operator.class);
        final Operator recycled = read(write(fromStatic, Operator.class, resourceName), Operator.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOrder() throws Exception {
        final String resourceName = "order";

        final Order resource = findModelEntryObject(resourceName);
        final Order fromStatic = read(readFromResource(resourceName), Order.class);
        final Order recycled = read(write(fromStatic, Order.class, resourceName), Order.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteP() throws Exception {
        final String resourceName = "p";

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class, resourceName), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePAnd() throws Exception {
        final String resourceName = "pand";

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class, resourceName), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePath() throws Exception {
        final String resourceName = "path";

        final Path resource = findModelEntryObject(resourceName);
        final Path fromStatic = read(readFromResource(resourceName), Path.class);
        final Path recycled = read(write(fromStatic, Path.class, resourceName), Path.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePeriod() throws Exception {
        final String resourceName = "period";

        final Period resource = findModelEntryObject(resourceName);
        final Period fromStatic = read(readFromResource(resourceName), Period.class);
        final Period recycled = read(write(fromStatic, Period.class, resourceName), Period.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePop() throws Exception {
        final String resourceName = "pop";

        final Pop resource = findModelEntryObject(resourceName);
        final Pop fromStatic = read(readFromResource(resourceName), Pop.class);
        final Pop recycled = read(write(fromStatic, Pop.class, resourceName), Pop.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePOr() throws Exception {
        final String resourceName = "por";

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class, resourceName), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteProperty() throws Exception {
        final String resourceName = "property";

        final Property resource = findModelEntryObject(resourceName);
        final Property fromStatic = read(readFromResource(resourceName), Property.class);
        final Property recycled = read(write(fromStatic, Property.class, resourceName), Property.class);
        assertNotSame(fromStatic, recycled);
        assertProperty(resource, fromStatic);
        assertProperty(resource, recycled);
    }

    @Test
    public void shouldReadWriteScope() throws Exception {
        final String resourceName = "scope";

        final Scope resource = findModelEntryObject(resourceName);
        final Scope fromStatic = read(readFromResource(resourceName), Scope.class);
        final Scope recycled = read(write(fromStatic, Scope.class, resourceName), Scope.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteSet() throws Exception {
        final String resourceName = "set";

        assumeThat("GraphSON v2 didn't explicitly have Set as a type",
                is(this.getClass().equals(GraphSONTypedCompatibilityTest.class) && getCompatibility().equals("v2")));

        final Set resource = findModelEntryObject(resourceName);
        final Set fromStatic = read(readFromResource(resourceName), Set.class);
        final Set recycled = read(write(fromStatic, Set.class, resourceName), Set.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteShort() throws Exception {
        final String resourceName = "short";

        final Short resource = findModelEntryObject(resourceName);
        final Short fromStatic = read(readFromResource(resourceName), Short.class);
        final Short recycled = read(write(fromStatic, Short.class, resourceName), Short.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteStandardRequestMessage() throws Exception {
        final String resourceName = "standardrequest";

        assumeThat("GraphBinary does not test this because Request/ResponseMessage are not actual GraphBinary types",
                this.getClass().equals(GraphBinaryCompatibilityTest.class),
                is(false));
        assumeThat("RequestMessageV4 was supported starting in GraphSONv4",
                getCompatibility(),
                is("v4"));

        final RequestMessageV4 resource = findModelEntryObject(resourceName);
        final RequestMessageV4 fromStatic = read(readFromResource(resourceName), RequestMessageV4.class);
        final RequestMessageV4 recycled = read(write(fromStatic, RequestMessageV4.class, resourceName), RequestMessageV4.class);
        assertNotSame(fromStatic, recycled);
        assertRequestMessage(resource, fromStatic, recycled);
    }

    @Test
    public void shouldReadWriteStandardExceptionResult() throws Exception {
        final String resourceName = "standardexceptionresult";

        assumeThat("GraphBinary does not test this because Request/ResponseMessage are not actual GraphBinary types",
                this.getClass().equals(GraphBinaryCompatibilityTest.class),
                is(false));
        assumeThat("ResponseMessageV4 was supported starting in GraphSONv4",
                getCompatibility(),
                is("v4"));

        final GraphSONMessageSerializerV4 gsSerializer = new GraphSONMessageSerializerV4();
        final ByteBuf resourceInBytes = gsSerializer.writeHeader(findModelEntryObject(resourceName), new UnpooledByteBufAllocator(false));
        final ResponseMessageV4 resource = read(resourceInBytes.array(), ResponseMessageV4.class);
        final ResponseMessageV4 fromStatic = read(readFromResource(resourceName), ResponseMessageV4.class);
        final ResponseMessageV4 recycled = read(write(fromStatic, HashMap.class, resourceName), ResponseMessageV4.class);
        assertNotSame(fromStatic, recycled);
        assertResponseMessage(resource, fromStatic, recycled);
        resourceInBytes.release();
    }

    @Test
    public void shouldReadWriteStandardResult() throws Exception {
        final String resourceName = "standardresult";

        assumeThat("GraphBinary does not test this because Request/ResponseMessage are not actual GraphBinary types",
                this.getClass().equals(GraphBinaryCompatibilityTest.class),
                is(false));
        assumeThat("ResponseMessageV4 was supported starting in GraphSONv4",
                getCompatibility(),
                is("v4"));

        final GraphSONMessageSerializerV4 gsSerializer = new GraphSONMessageSerializerV4();
        final ByteBuf resourceInBytes = gsSerializer.writeHeader(findModelEntryObject(resourceName), new UnpooledByteBufAllocator(false));
        final ResponseMessageV4 resource = read(resourceInBytes.array(), ResponseMessageV4.class);
        final ResponseMessageV4 fromStatic = read(readFromResource(resourceName), ResponseMessageV4.class);
        final ResponseMessageV4 recycled = read(write(fromStatic, HashMap.class, resourceName), ResponseMessageV4.class);
        assertNotSame(fromStatic, recycled);
        assertResponseMessage(resource, fromStatic, recycled);
    }

    @Test
    public void shouldReadWriteT() throws Exception {
        final String resourceName = "t";

        final T resource = findModelEntryObject(resourceName);
        final T fromStatic = read(readFromResource(resourceName), T.class);
        final T recycled = read(write(fromStatic, T.class, resourceName), T.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTextP() throws Exception {
        final String resourceName = "textp";

        final TextP resource = findModelEntryObject(resourceName);
        final TextP fromStatic = read(readFromResource(resourceName), TextP.class);
        final TextP recycled = read(write(fromStatic, TextP.class, resourceName), TextP.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTimestamp() throws Exception {
        final String resourceName = "timestamp";

        final Timestamp resource = findModelEntryObject(resourceName);
        final Timestamp fromStatic = read(readFromResource(resourceName), Timestamp.class);
        final Timestamp recycled = read(write(fromStatic, Timestamp.class, resourceName), Timestamp.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteTinkerGraph() throws Exception {
        final String resourceName = "tinkergraph";

        final TinkerGraph fromStatic = read(readFromResource(resourceName), TinkerGraph.class);
        final TinkerGraph recycled = read(write(fromStatic, TinkerGraph.class, resourceName), TinkerGraph.class);
        assertNotSame(fromStatic, recycled);

        IoTest.assertCrewGraph(fromStatic, false);
        IoTest.assertCrewGraph(recycled, false);
    }

    @Test
    public void shouldReadWriteTraversalMetrics() throws Exception {
        final String resourceName = "traversalmetrics";

        final TraversalMetrics resource = findModelEntryObject(resourceName);
        final TraversalMetrics fromStatic = read(readFromResource(resourceName), TraversalMetrics.class);
        final TraversalMetrics recycled = read(write(fromStatic, TraversalMetrics.class, resourceName), TraversalMetrics.class);
        assertNotSame(fromStatic, recycled);

        // need to assert against each other since the model version can change between test runs as it is dynamically
        // generated
        assertEquals(recycled.getDuration(TimeUnit.MILLISECONDS), fromStatic.getDuration(TimeUnit.MILLISECONDS));
        final Collection<? extends Metrics> resourceMetrics = resource.getMetrics();
        resourceMetrics.forEach(m -> {
            assertEquals(recycled.getMetrics(m.getId()).getAnnotations(), fromStatic.getMetrics(m.getId()).getAnnotations());
            assertEquals(recycled.getMetrics(m.getId()).getName(), fromStatic.getMetrics(m.getId()).getName());
            assertEquals(recycled.getMetrics(m.getId()).getCounts(), fromStatic.getMetrics(m.getId()).getCounts());
        });
    }

    @Test
    public void shouldReadWriteTraverser() throws Exception {
        final String resourceName = "traverser";

        final Traverser resource = findModelEntryObject(resourceName);
        final Traverser fromStatic = read(readFromResource(resourceName), Traverser.class);
        final Traverser recycled = read(write(fromStatic, Traverser.class, resourceName), Traverser.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.bulk(), recycled.bulk());
        assertEquals(resource.bulk(), fromStatic.bulk());
        assertEquals(resource.get(), recycled.get());
        assertEquals(resource.get(), fromStatic.get());
        assertVertex((Vertex) resource.get(), (Vertex) recycled.get());
        assertVertex((Vertex) resource.get(), (Vertex) fromStatic.get());
    }

    @Test
    public void shouldReadWriteUUID() throws Exception {
        final String resourceName = "uuid";

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
        final String resourceName = "vertex";

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
        final String resourceName = "vertexproperty";

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
    public void shouldReadWriteYear() throws Exception {
        final String resourceName = "year";

        final Year resource = findModelEntryObject(resourceName);
        final Year fromStatic = read(readFromResource(resourceName), Year.class);
        final Year recycled = read(write(fromStatic, Year.class, resourceName), Year.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteYearMonth() throws Exception {
        final String resourceName = "yearmonth";

        final YearMonth resource = findModelEntryObject(resourceName);
        final YearMonth fromStatic = read(readFromResource(resourceName), YearMonth.class);
        final YearMonth recycled = read(write(fromStatic, YearMonth.class, resourceName), YearMonth.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteZoneDateTime() throws Exception {
        final String resourceName = "zoneddatetime";

        // seem to recall this was a known issue for GraphBinary. not going to fuss with it though since the
        // longer term plan is to likely drop these jvm only types.
        assumeThat("GraphBinary doesn't quite serialize ZoneDateTime for a perfect round trip",
                is(getClass().equals(GraphBinaryCompatibilityTest.class)));

        final ZonedDateTime resource = findModelEntryObject(resourceName);
        final ZonedDateTime fromStatic = read(readFromResource(resourceName), ZonedDateTime.class);
        final ZonedDateTime recycled = read(write(fromStatic, ZonedDateTime.class, resourceName), ZonedDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteZoneOffset() throws Exception {
        final String resourceName = "zoneoffset";

        final ZoneOffset resource = findModelEntryObject(resourceName);
        final ZoneOffset fromStatic = read(readFromResource(resourceName), ZoneOffset.class);
        final ZoneOffset recycled = read(write(fromStatic, ZoneOffset.class, resourceName), ZoneOffset.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }
}
