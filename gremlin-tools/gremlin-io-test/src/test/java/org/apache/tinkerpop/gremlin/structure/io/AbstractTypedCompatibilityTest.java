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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
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
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractTypedCompatibilityTest extends AbstractCompatibilityTest {

    public abstract <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception;

    public abstract byte[] write(final Object o, final Class<?> clazz) throws Exception;

    @Test
    @org.junit.Ignore
    public void shouldReadWriteAuthenticationChallenge() throws Exception {
        final String resourceName = "authenticationchallenge";
        assumeCompatibility(resourceName);

        final ResponseMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getStatus().getCode().getValue(), ((Map) recycled.get("status")).get("code"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getStatus().getCode().getValue(), ((Map) fromStatic.get("status")).get("code"));
    }

    @Test
    public void shouldReadWriteAuthenticationResponse() throws Exception {
        final String resourceName = "authenticationresponse";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("saslMechanism"), ((Map) recycled.get("args")).get("saslMechanism"));
        assertEquals(resource.getArgs().get("sasl"), ((Map) recycled.get("args")).get("sasl"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().get("saslMechanism"), ((Map) fromStatic.get("args")).get("saslMechanism"));
        assertEquals(resource.getArgs().get("sasl"), ((Map) fromStatic.get("args")).get("sasl"));
    }

    @Test
    public void shouldReadWriteBarrier() throws Exception {
        final String resourceName = "barrier";
        assumeCompatibility(resourceName);

        final SackFunctions.Barrier resource = findModelEntryObject(resourceName);
        final SackFunctions.Barrier fromStatic = read(getCompatibility().readFromResource(resourceName), SackFunctions.Barrier.class);
        final SackFunctions.Barrier recycled = read(write(fromStatic, SackFunctions.Barrier.class), SackFunctions.Barrier.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBigDecimal() throws Exception {
        final String resourceName = "bigdecimal";
        assumeCompatibility(resourceName);

        final BigDecimal resource = findModelEntryObject(resourceName);
        final BigDecimal fromStatic = read(getCompatibility().readFromResource(resourceName), BigDecimal.class);
        final BigDecimal recycled = read(write(fromStatic, BigDecimal.class), BigDecimal.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBigInteger() throws Exception {
        final String resourceName = "biginteger";
        assumeCompatibility(resourceName);

        final BigInteger resource = findModelEntryObject(resourceName);
        final BigInteger fromStatic = read(getCompatibility().readFromResource(resourceName), BigInteger.class);
        final BigInteger recycled = read(write(fromStatic, BigInteger.class), BigInteger.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBinding() throws Exception {
        final String resourceName = "binding";
        assumeCompatibility(resourceName);

        final Bytecode.Binding resource = findModelEntryObject(resourceName);
        final Bytecode.Binding fromStatic = read(getCompatibility().readFromResource(resourceName), Bytecode.Binding.class);
        final Bytecode.Binding recycled = read(write(fromStatic, Bytecode.Binding.class), Bytecode.Binding.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteByte() throws Exception {
        final String resourceName = "byte";
        assumeCompatibility(resourceName);

        final Byte resource = findModelEntryObject(resourceName);
        final Byte fromStatic = read(getCompatibility().readFromResource(resourceName), Byte.class);
        final Byte recycled = read(write(fromStatic, Byte.class), Byte.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteByteBuffer() throws Exception {
        final String resourceName = "bytebuffer";
        assumeCompatibility(resourceName);

        final ByteBuffer resource = findModelEntryObject(resourceName);
        final ByteBuffer fromStatic = read(getCompatibility().readFromResource(resourceName), ByteBuffer.class);
        final ByteBuffer recycled = read(write(fromStatic, ByteBuffer.class), ByteBuffer.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteBytecode() throws Exception {
        final String resourceName = "bytecode";
        assumeCompatibility(resourceName);

        final Bytecode fromStatic = read(getCompatibility().readFromResource(resourceName), Bytecode.class);
        final Bytecode recycled = read(write(fromStatic, Bytecode.class), Bytecode.class);
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
        assumeCompatibility(resourceName);

        final VertexProperty.Cardinality resource = findModelEntryObject(resourceName);
        final VertexProperty.Cardinality fromStatic = read(getCompatibility().readFromResource(resourceName), VertexProperty.Cardinality.class);
        final VertexProperty.Cardinality recycled = read(write(fromStatic, VertexProperty.Cardinality.class), VertexProperty.Cardinality.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteChar() throws Exception {
        final String resourceName = "char";
        assumeCompatibility(resourceName);

        final Character resource = findModelEntryObject(resourceName);
        final Character fromStatic = read(getCompatibility().readFromResource(resourceName), Character.class);
        final Character recycled = read(write(fromStatic, Character.class), Character.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteClass() throws Exception {
        final String resourceName = "class";
        assumeCompatibility(resourceName);

        final Class resource = findModelEntryObject(resourceName);
        final Class fromStatic = read(getCompatibility().readFromResource(resourceName), Class.class);
        final Class recycled = read(write(fromStatic, Class.class), Class.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteColumn() throws Exception {
        final String resourceName = "column";
        assumeCompatibility(resourceName);

        final Column resource = findModelEntryObject(resourceName);
        final Column fromStatic = read(getCompatibility().readFromResource(resourceName), Column.class);
        final Column recycled = read(write(fromStatic, Column.class), Column.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDate() throws Exception {
        final String resourceName = "date";
        assumeCompatibility(resourceName);

        final Date resource = findModelEntryObject(resourceName);
        final Date fromStatic = read(getCompatibility().readFromResource(resourceName), Date.class);
        final Date recycled = read(write(fromStatic, Date.class), Date.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDirection() throws Exception {
        final String resourceName = "direction";
        assumeCompatibility(resourceName);

        final Direction resource = findModelEntryObject(resourceName);
        final Direction fromStatic = read(getCompatibility().readFromResource(resourceName), Direction.class);
        final Direction recycled = read(write(fromStatic, Direction.class), Direction.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDouble() throws Exception {
        final String resourceName = "double";
        assumeCompatibility(resourceName);

        final Double resource = findModelEntryObject(resourceName);
        final Double fromStatic = read(getCompatibility().readFromResource(resourceName), Double.class);
        final Double recycled = read(write(fromStatic, Double.class), Double.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteDuration() throws Exception {
        final String resourceName = "duration";
        assumeCompatibility(resourceName);

        final Duration resource = findModelEntryObject(resourceName);
        final Duration fromStatic = read(getCompatibility().readFromResource(resourceName), Duration.class);
        final Duration recycled = read(write(fromStatic, Duration.class), Duration.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteEdge() throws Exception {
        final String resourceName = "edge";
        assumeCompatibility(resourceName);

        final Edge resource = findModelEntryObject(resourceName);
        final Edge fromStatic = read(getCompatibility().readFromResource(resourceName), Edge.class);
        final Edge recycled = (Edge) read(write(fromStatic, Edge.class), getCompatibility().resolve(Edge.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteFloat() throws Exception {
        final String resourceName = "float";
        assumeCompatibility(resourceName);

        final Float resource = findModelEntryObject(resourceName);
        final Float fromStatic = read(getCompatibility().readFromResource(resourceName), Float.class);
        final Float recycled = read(write(fromStatic, Float.class), Float.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteINetAddress() throws Exception {
        final String resourceName = "inetaddress";
        assumeCompatibility(resourceName);

        final InetAddress resource = findModelEntryObject(resourceName);
        final InetAddress fromStatic = read(getCompatibility().readFromResource(resourceName), InetAddress.class);
        final InetAddress recycled = read(write(fromStatic, Float.class), InetAddress.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteInstant() throws Exception {
        final String resourceName = "instant";
        assumeCompatibility(resourceName);

        final Instant resource = findModelEntryObject(resourceName);
        final Instant fromStatic = read(getCompatibility().readFromResource(resourceName), Instant.class);
        final Instant recycled = read(write(fromStatic, Instant.class), Instant.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteInteger() throws Exception {
        final String resourceName = "integer";
        assumeCompatibility(resourceName);

        final Integer resource = findModelEntryObject(resourceName);
        final Integer fromStatic = read(getCompatibility().readFromResource(resourceName), Integer.class);
        final Integer recycled = read(write(fromStatic, Integer.class), Integer.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLambda() throws Exception {
        final String resourceName = "lambda";
        assumeCompatibility(resourceName);

        final Lambda resource = findModelEntryObject(resourceName);
        final Lambda fromStatic = read(getCompatibility().readFromResource(resourceName), Lambda.class);
        final Lambda recycled = read(write(fromStatic, Lambda.class), Lambda.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalDate() throws Exception {
        final String resourceName = "localdate";
        assumeCompatibility(resourceName);

        final LocalDate resource = findModelEntryObject(resourceName);
        final LocalDate fromStatic = read(getCompatibility().readFromResource(resourceName), LocalDate.class);
        final LocalDate recycled = read(write(fromStatic, LocalDate.class), LocalDate.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalDateTime() throws Exception {
        final String resourceName = "localdatetime";
        assumeCompatibility(resourceName);

        final LocalDateTime resource = findModelEntryObject(resourceName);
        final LocalDateTime fromStatic = read(getCompatibility().readFromResource(resourceName), LocalDateTime.class);
        final LocalDateTime recycled = read(write(fromStatic, LocalDateTime.class), LocalDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLocalTime() throws Exception {
        final String resourceName = "localtime";
        assumeCompatibility(resourceName);

        final LocalTime resource = findModelEntryObject(resourceName);
        final LocalTime fromStatic = read(getCompatibility().readFromResource(resourceName), LocalTime.class);
        final LocalTime recycled = read(write(fromStatic, LocalTime.class), LocalTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteLong() throws Exception {
        final String resourceName = "long";
        assumeCompatibility(resourceName);

        final Long resource = findModelEntryObject(resourceName);
        final Long fromStatic = read(getCompatibility().readFromResource(resourceName), Long.class);
        final Long recycled = read(write(fromStatic, Long.class), Long.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteMetrics() throws Exception {
        final String resourceName = "metrics";
        assumeCompatibility(resourceName);

        final Metrics fromStatic = (Metrics) read(getCompatibility().readFromResource(resourceName), getCompatibility().resolve(Metrics.class));
        final Metrics recycled = (Metrics) read(write(fromStatic, Metrics.class), getCompatibility().resolve(Metrics.class));
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
        assumeCompatibility(resourceName);

        final MonthDay resource = findModelEntryObject(resourceName);
        final MonthDay fromStatic = read(getCompatibility().readFromResource(resourceName), MonthDay.class);
        final MonthDay recycled = read(write(fromStatic, MonthDay.class), MonthDay.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOffsetDateTime() throws Exception {
        final String resourceName = "offsetdatetime";
        assumeCompatibility(resourceName);

        final OffsetDateTime resource = findModelEntryObject(resourceName);
        final OffsetDateTime fromStatic = read(getCompatibility().readFromResource(resourceName), OffsetDateTime.class);
        final OffsetDateTime recycled = read(write(fromStatic, OffsetDateTime.class), OffsetDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOffsetTime() throws Exception {
        final String resourceName = "offsettime";
        assumeCompatibility(resourceName);

        final OffsetTime resource = findModelEntryObject(resourceName);
        final OffsetTime fromStatic = read(getCompatibility().readFromResource(resourceName), OffsetTime.class);
        final OffsetTime recycled = read(write(fromStatic, OffsetTime.class), OffsetTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteOperator() throws Exception {
        final String resourceName = "operator";
        assumeCompatibility(resourceName);

        final Operator resource = findModelEntryObject(resourceName);
        final Operator fromStatic = read(getCompatibility().readFromResource(resourceName), Operator.class);
        final Operator recycled = read(write(fromStatic, Operator.class), Operator.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }









    @Test
    public void shouldReadWriteTimestamp() throws Exception {
        final String resourceName = "timestamp";
        assumeCompatibility(resourceName);

        final Timestamp resource = findModelEntryObject(resourceName);
        final Timestamp fromStatic = read(getCompatibility().readFromResource(resourceName), Timestamp.class);
        final Timestamp recycled = read(write(fromStatic, Timestamp.class), Timestamp.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteZoneDateTime() throws Exception {
        final String resourceName = "zoneddatetime";
        assumeCompatibility(resourceName);

        final ZonedDateTime resource = findModelEntryObject(resourceName);
        final ZonedDateTime fromStatic = read(getCompatibility().readFromResource(resourceName), ZonedDateTime.class);
        final ZonedDateTime recycled = read(write(fromStatic, ZonedDateTime.class), ZonedDateTime.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }
}
