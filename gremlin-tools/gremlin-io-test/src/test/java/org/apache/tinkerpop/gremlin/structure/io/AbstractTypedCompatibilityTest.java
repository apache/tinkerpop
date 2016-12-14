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

import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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
        assumeCompatibility("authenticationchallenge");

        final HashMap fromStatic = read(getCompatibility().readFromResource("authenticationchallenge"), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals("41d2e28a-20a4-4ab0-b379-d810dede3786", recycled.get("requestId"));
        assertEquals(ResponseStatusCode.AUTHENTICATE.getValue(), ((Map) recycled.get("status")).get("code"));
    }

    @Test
    public void shouldReadWriteAuthenticationResponse() throws Exception {
        assumeCompatibility("authenticationresponse");

        final HashMap fromStatic = read(getCompatibility().readFromResource("authenticationresponse"), HashMap.class);
        final HashMap recycled = read(write(fromStatic, HashMap.class), HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals("cb682578-9d92-4499-9ebc-5c6aa73c5397", recycled.get("requestId"));
        assertEquals("authentication", recycled.get("op"));
        assertEquals("", recycled.get("processor"));
        assertEquals("PLAIN", ((Map) recycled.get("args")).get("saslMechanism"));
        assertEquals("AHN0ZXBocGhlbgBwYXNzd29yZA==", ((Map) recycled.get("args")).get("sasl"));
    }

    @Test
    public void shouldReadWriteBarrier() throws Exception {
        assumeCompatibility("barrier");

        final SackFunctions.Barrier fromStatic = read(getCompatibility().readFromResource("barrier"), SackFunctions.Barrier.class);
        final SackFunctions.Barrier recycled = read(write(fromStatic, SackFunctions.Barrier.class), SackFunctions.Barrier.class);
        assertEquals(fromStatic, recycled);
    }

    @Test
    public void shouldReadWriteBigDecimal() throws Exception {
        assumeCompatibility("bigdecimal");

        final BigDecimal fromStatic = read(getCompatibility().readFromResource("bigdecimal"), BigDecimal.class);
        final BigDecimal recycled = read(write(fromStatic, BigDecimal.class), BigDecimal.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
    }
}
