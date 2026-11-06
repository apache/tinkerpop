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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.server.pdt.TinkerId;
import org.apache.tinkerpop.gremlin.server.pdt.Uint32;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for PrimitivePDT round-trip through gremlin-server.
 * Exercises Uint32 (numeric), TinkerId (non-numeric), and a composite containing a primitive.
 */
public class GremlinServerPrimitivePdtIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Cluster cluster;
    private Client client;

    @Before
    public void openConnection() {
        cluster = TestClientFactory.build().create();
        client = cluster.connect();
    }

    @After
    public void closeConnection() {
        if (cluster != null) cluster.close();
    }

    @Test
    public void shouldRoundTripUint32PrimitivePdt() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"Uint32\",\"4294967295\"))").all().get();

        assertEquals(1, results.size());
        final Object obj = results.get(0).getObject();
        // With Uint32Adapter on classpath, the reader hydrates to Uint32
        assertThat(obj, instanceOf(Uint32.class));
        assertEquals(4294967295L, ((Uint32) obj).getValue());
    }

    @Test
    public void shouldRoundTripUint32ZeroValue() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"Uint32\",\"0\"))").all().get();

        assertEquals(1, results.size());
        final Object obj = results.get(0).getObject();
        assertThat(obj, instanceOf(Uint32.class));
        assertEquals(0L, ((Uint32) obj).getValue());
    }

    @Test
    public void shouldRoundTripTinkerIdPrimitivePdt() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"TinkerId\",\"abc-123-def\"))").all().get();

        assertEquals(1, results.size());
        final Object obj = results.get(0).getObject();
        // With TinkerIdAdapter on classpath, the reader hydrates to TinkerId
        assertThat(obj, instanceOf(TinkerId.class));
        assertEquals("abc-123-def", ((TinkerId) obj).getId());
    }

    @Test
    public void shouldRoundTripPrimitiveNestedInComposite() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"Measurement\",[\"unit\":\"meters\",\"quantity\":PDT(\"Uint32\",\"100\")]))").all().get();

        assertEquals(1, results.size());
        final Object obj = results.get(0).getObject();
        // Measurement has no adapter registered, so outer is raw CompositePDT
        assertThat(obj, instanceOf(CompositePDT.class));
        final CompositePDT pdt = (CompositePDT) obj;
        assertEquals("Measurement", pdt.getName());
        assertEquals("meters", pdt.getFields().get("unit"));
        // Nested primitive is hydrated to Uint32
        final Object quantity = pdt.getFields().get("quantity");
        assertThat(quantity, instanceOf(Uint32.class));
        assertEquals(100L, ((Uint32) quantity).getValue());
    }

    @Test
    public void shouldRoundTripMultiplePrimitivePdtsInCollection() throws Exception {
        final List<Result> results = client.submit(
                "g.inject([PDT(\"Uint32\",\"42\"),PDT(\"TinkerId\",\"x-1\")])").all().get();

        assertEquals(1, results.size());
        final List<?> list = (List<?>) results.get(0).getObject();
        assertEquals(2, list.size());
        assertThat(list.get(0), instanceOf(Uint32.class));
        assertEquals(42L, ((Uint32) list.get(0)).getValue());
        assertThat(list.get(1), instanceOf(TinkerId.class));
        assertEquals("x-1", ((TinkerId) list.get(1)).getId());
    }
}
