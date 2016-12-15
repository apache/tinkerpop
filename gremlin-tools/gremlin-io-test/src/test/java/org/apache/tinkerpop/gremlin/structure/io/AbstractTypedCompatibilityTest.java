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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
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
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
        assertEquals(resource.id(), recycled.id());
        assertEquals(resource.label(), recycled.label());
        assertEquals(resource.inVertex().id(), recycled.inVertex().id());
        assertEquals(resource.outVertex().id(), recycled.outVertex().id());
        assertEquals(resource.inVertex().label(), recycled.inVertex().label());
        assertEquals(resource.outVertex().label(), recycled.outVertex().label());
        assertEquals(IteratorUtils.count(resource.properties()), IteratorUtils.count(recycled.properties()));
        assertEquals((int) resource.value("since"), (int) recycled.value("since"));
        assertEquals(resource.id(), fromStatic.id());
        assertEquals(resource.label(), fromStatic.label());
        assertEquals(resource.inVertex().id(), fromStatic.inVertex().id());
        assertEquals(resource.outVertex().id(), fromStatic.outVertex().id());
        assertEquals(resource.inVertex().label(), fromStatic.inVertex().label());
        assertEquals(resource.outVertex().label(), fromStatic.outVertex().label());
        assertEquals(IteratorUtils.count(resource.properties()), IteratorUtils.count(fromStatic.properties()));
        assertEquals((int) resource.value("since"), (int) fromStatic.value("since"));
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
    public void shouldReadWriteOrder() throws Exception {
        final String resourceName = "order";
        assumeCompatibility(resourceName);

        final Order resource = findModelEntryObject(resourceName);
        final Order fromStatic = read(getCompatibility().readFromResource(resourceName), Order.class);
        final Order recycled = read(write(fromStatic, Order.class), Order.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteP() throws Exception {
        final String resourceName = "p";
        assumeCompatibility(resourceName);

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(getCompatibility().readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePAnd() throws Exception {
        final String resourceName = "pand";
        assumeCompatibility(resourceName);

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(getCompatibility().readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePath() throws Exception {
        final String resourceName = "path";
        assumeCompatibility(resourceName);

        final Path resource = findModelEntryObject(resourceName);
        final Path fromStatic = read(getCompatibility().readFromResource(resourceName), Path.class);
        final Path recycled = (Path) read(write(fromStatic, Path.class), getCompatibility().resolve(Path.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePeriod() throws Exception {
        final String resourceName = "period";
        assumeCompatibility(resourceName);

        final Period resource = findModelEntryObject(resourceName);
        final Period fromStatic = read(getCompatibility().readFromResource(resourceName), Period.class);
        final Period recycled = read(write(fromStatic, Period.class), Period.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePop() throws Exception {
        final String resourceName = "pop";
        assumeCompatibility(resourceName);

        final Pop resource = findModelEntryObject(resourceName);
        final Pop fromStatic = read(getCompatibility().readFromResource(resourceName), Pop.class);
        final Pop recycled = read(write(fromStatic, Pop.class), Pop.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWritePOr() throws Exception {
        final String resourceName = "por";
        assumeCompatibility(resourceName);

        final P resource = findModelEntryObject(resourceName);
        final P fromStatic = read(getCompatibility().readFromResource(resourceName), P.class);
        final P recycled = read(write(fromStatic, P.class), P.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteProperty() throws Exception {
        final String resourceName = "property";
        assumeCompatibility(resourceName);

        final Property resource = findModelEntryObject(resourceName);
        final Property fromStatic = read(getCompatibility().readFromResource(resourceName), Property.class);
        final Property recycled = (Property) read(write(fromStatic, Property.class), getCompatibility().resolve(Property.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.key(), recycled.key());
        assertEquals(resource.value(), recycled.value());
        assertEquals(resource.key(), fromStatic.key());
        assertEquals(resource.value(), fromStatic.value());
    }

    @Test
    public void shouldReadWriteScope() throws Exception {
        final String resourceName = "scope";
        assumeCompatibility(resourceName);

        final Scope resource = findModelEntryObject(resourceName);
        final Scope fromStatic = read(getCompatibility().readFromResource(resourceName), Scope.class);
        final Scope recycled = read(write(fromStatic, Scope.class), Scope.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteSessionClose() throws Exception {
        final String resourceName = "sessionclose";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("session"), ((Map) recycled.get("args")).get("session"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().get("session"), ((Map) fromStatic.get("args")).get("session"));
    }

    @Test
    public void shouldReadWriteSessionEval() throws Exception {
        final String resourceName = "sessioneval";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("session"), ((Map) recycled.get("args")).get("session"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) recycled.get("args")).get("bindings")).get("x"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getArgs().get("session"), ((Map) fromStatic.get("args")).get("session"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) fromStatic.get("args")).get("bindings")).get("x"));
    }

    @Test
    public void shouldReadWriteSessionEvalAliased() throws Exception {
        final String resourceName = "sessionevalaliased";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("session"), ((Map) recycled.get("args")).get("session"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("aliases")).get("g"), ((Map) ((Map) recycled.get("args")).get("aliases")).get("g"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) recycled.get("args")).get("bindings")).get("x"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getArgs().get("session"), ((Map) fromStatic.get("args")).get("session"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("aliases")).get("g"), ((Map) ((Map) fromStatic.get("args")).get("aliases")).get("g"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) fromStatic.get("args")).get("bindings")).get("x"));
    }

    @Test
    public void shouldReadWriteSessionlessEval() throws Exception {
        final String resourceName = "sessionlesseval";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) recycled.get("args")).get("bindings")).get("x"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) fromStatic.get("args")).get("bindings")).get("x"));
    }

    @Test
    public void shouldReadWriteSessionlessEvalAliased() throws Exception {
        final String resourceName = "sessionlessevalaliased";
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("aliases")).get("g"), ((Map) ((Map) recycled.get("args")).get("aliases")).get("g"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) recycled.get("args")).get("bindings")).get("x"));
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(((Map) resource.getArgs().get("aliases")).get("g"), ((Map) ((Map) fromStatic.get("args")).get("aliases")).get("g"));
        assertEquals(((Map) resource.getArgs().get("bindings")).get("x"), ((Map) ((Map) fromStatic.get("args")).get("bindings")).get("x"));
    }

    @Test
    public void shouldReadWriteShort() throws Exception {
        final String resourceName = "short";
        assumeCompatibility(resourceName);

        final Short resource = findModelEntryObject(resourceName);
        final Short fromStatic = read(getCompatibility().readFromResource(resourceName), Short.class);
        final Short recycled = read(write(fromStatic, Short.class), Short.class);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteStandardResult() throws Exception {
        final String resourceName = "standardresult";
        assumeCompatibility(resourceName);

        // todo: incomplete asserts - none of this is consistent right now
        final ResponseMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        final List<DetachedVertex> resourceVertices = (List<DetachedVertex>) resource.getResult().getData();
        assertEquals(resource.getRequestId(), recycled.get("requestId"));
        assertEquals(resource.getStatus().getCode().getValue(), ((Map) recycled.get("status")).get("code"));
        assertEquals(resourceVertices.size(), ((List) ((Map) recycled.get("result"))).size());
        assertEquals(resource.getRequestId(), fromStatic.get("requestId"));
        assertEquals(resource.getStatus().getCode().getValue(), ((Map) fromStatic.get("status")).get("code"));
    }

    @Test
    public void shouldReadWriteStarGraph() throws Exception {
        final String resourceName = "stargraph";
        assumeCompatibility(resourceName);

        // todo: more asserts
        final StarGraph resource = findModelEntryObject(resourceName);
        final StarGraph fromStatic = read(getCompatibility().readFromResource(resourceName), StarGraph.class);
        final StarGraph recycled = read(write(fromStatic, StarGraph.class), StarGraph.class);
        assertNotSame(fromStatic.getStarVertex(), recycled.getStarVertex());
        assertEquals(fromStatic.getStarVertex(), recycled.getStarVertex());
        assertEquals(resource.getStarVertex(), fromStatic.getStarVertex());
        assertEquals(resource.getStarVertex(), recycled.getStarVertex());
        assertEquals(fromStatic.getStarVertex().label(), recycled.getStarVertex().label());
        assertEquals(fromStatic.getStarVertex().id(), recycled.getStarVertex().id());
        assertEquals(IteratorUtils.count(fromStatic.getStarVertex().properties()), IteratorUtils.count(recycled.getStarVertex().properties()));
        assertEquals(fromStatic.getStarVertex().property("name").value(), recycled.getStarVertex().property("name").value());
        assertEquals(fromStatic.getStarVertex().property("name").id(), recycled.getStarVertex().property("name").id());
        assertEquals(IteratorUtils.count(fromStatic.getStarVertex().edges(Direction.BOTH)), IteratorUtils.count(recycled.getStarVertex().edges(Direction.BOTH)));
    }

    @Test
    public void shouldReadWriteT() throws Exception {
        final String resourceName = "t";
        assumeCompatibility(resourceName);

        final T resource = findModelEntryObject(resourceName);
        final T fromStatic = read(getCompatibility().readFromResource(resourceName), T.class);
        final T recycled = read(write(fromStatic, T.class), T.class);
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
    public void shouldReadWriteTinkerGraph() throws Exception {
        final String resourceName = "tinkergraph";
        assumeCompatibility(resourceName);

        final TinkerGraph fromStatic = read(getCompatibility().readFromResource(resourceName), TinkerGraph.class);
        final TinkerGraph recycled = read(write(fromStatic, TinkerGraph.class), TinkerGraph.class);
        assertNotSame(fromStatic, recycled);

        IoTest.assertCrewGraph(fromStatic, false);
        IoTest.assertCrewGraph(recycled, false);
    }

    @Test
    public void shouldReadWriteTraversalMetrics() throws Exception {
        final String resourceName = "traversalmetrics";
        assumeCompatibility(resourceName);

        final TraversalMetrics resource = findModelEntryObject(resourceName);
        final TraversalMetrics fromStatic = (TraversalMetrics) read(getCompatibility().readFromResource(resourceName), getCompatibility().resolve(TraversalMetrics.class));
        final TraversalMetrics recycled = (TraversalMetrics) read(write(fromStatic, TraversalMetrics.class), getCompatibility().resolve(TraversalMetrics.class));
        assertNotSame(fromStatic, recycled);

        // need to assert against each other since the model version can change between test runs as it is dyncamically
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
        assumeCompatibility(resourceName);

        final Traverser resource = findModelEntryObject(resourceName);
        final Traverser fromStatic = (Traverser) read(getCompatibility().readFromResource(resourceName), getCompatibility().resolve(Traverser.class));
        final Traverser recycled = (Traverser) read(write(fromStatic, Traverser.class), getCompatibility().resolve(Traverser.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.bulk(), recycled.bulk());
        assertEquals(resource.bulk(), fromStatic.bulk());
        assertEquals(resource.get(), recycled.get());
        assertEquals(resource.get(), fromStatic.get());

        // todo: more asserts on object itself
    }

    @Test
    public void shouldReadWriteTree() throws Exception {
        final String resourceName = "tree";
        assumeCompatibility(resourceName);

        final Tree resource = findModelEntryObject(resourceName);
        final Tree fromStatic = read(getCompatibility().readFromResource(resourceName), Tree.class);
        final Tree recycled = read(write(fromStatic, Tree.class), Tree.class);
        assertNotSame(fromStatic, recycled);

        // todo: more asserts on objects in three themselves
    }

    @Test
    public void shouldReadWriteUUID() throws Exception {
        final String resourceName = "uuid";
        assumeCompatibility(resourceName);

        final UUID resource = findModelEntryObject(resourceName);
        final UUID fromStatic = read(getCompatibility().readFromResource(resourceName), UUID.class);
        final UUID recycled = read(write(fromStatic, UUID.class), UUID.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteVertex() throws Exception {
        final String resourceName = "vertex";
        assumeCompatibility(resourceName);

        final Vertex resource = findModelEntryObject(resourceName);
        final Vertex fromStatic = read(getCompatibility().readFromResource(resourceName), Vertex.class);
        final Vertex recycled = (Vertex) read(write(fromStatic, Vertex.class), getCompatibility().resolve(Vertex.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertEquals(resource.id(), recycled.id());
        assertEquals(resource.label(), recycled.label());
        // todo: more asserts
    }

    @Test
    public void shouldReadWriteVertexProperty() throws Exception {
        final String resourceName = "vertexproperty";
        assumeCompatibility(resourceName);

        final VertexProperty resource = findModelEntryObject(resourceName);
        final VertexProperty fromStatic = read(getCompatibility().readFromResource(resourceName), VertexProperty.class);
        final VertexProperty recycled = (VertexProperty) read(write(fromStatic, VertexProperty.class), getCompatibility().resolve(VertexProperty.class));
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
        assertEquals(resource.id(), recycled.id());
        assertEquals(resource.label(), recycled.label());
        // todo: more asserts
    }

    @Test
    public void shouldReadWriteYear() throws Exception {
        final String resourceName = "year";
        assumeCompatibility(resourceName);

        final Year resource = findModelEntryObject(resourceName);
        final Year fromStatic = read(getCompatibility().readFromResource(resourceName), Year.class);
        final Year recycled = read(write(fromStatic, Year.class), Year.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }

    @Test
    public void shouldReadWriteYearMonth() throws Exception {
        final String resourceName = "yearmonth";
        assumeCompatibility(resourceName);

        final YearMonth resource = findModelEntryObject(resourceName);
        final YearMonth fromStatic = read(getCompatibility().readFromResource(resourceName), YearMonth.class);
        final YearMonth recycled = read(write(fromStatic, YearMonth.class), YearMonth.class);
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

    @Test
    public void shouldReadWriteZoneOffset() throws Exception {
        final String resourceName = "zoneoffset";
        assumeCompatibility(resourceName);

        final ZoneOffset resource = findModelEntryObject(resourceName);
        final ZoneOffset fromStatic = read(getCompatibility().readFromResource(resourceName), ZoneOffset.class);
        final ZoneOffset recycled = read(write(fromStatic, ZoneOffset.class), ZoneOffset.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource, fromStatic);
        assertEquals(resource, recycled);
    }
}
