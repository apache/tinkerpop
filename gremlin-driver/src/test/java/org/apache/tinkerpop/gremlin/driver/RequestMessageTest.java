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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessageTest {

    @Test
    public void shouldOverrideRequest() {
        final UUID request = UUID.randomUUID();
        final RequestMessage msg = RequestMessage.build("op").overrideRequestId(request).create();
        assertEquals(request, msg.getRequestId());
    }

    @Test
    public void shouldSetProcessor() {
        final RequestMessage msg = RequestMessage.build("op").processor("ppp").create();
        assertEquals("ppp", msg.getProcessor());
    }

    @Test
    public void shouldSetOpWithDefaults() {
        final RequestMessage msg = RequestMessage.build("op").create();
        Assert.assertEquals("", msg.getProcessor());    // standard op processor
        assertNotNull(msg.getRequestId());
        assertEquals("op", msg.getOp());
        assertNotNull(msg.getArgs());
    }

    @Test
    public void shouldReturnEmptyOptionalArg() {
        final RequestMessage msg = RequestMessage.build("op").create();
        assertFalse(msg.optionalArgs("test").isPresent());
    }

    @Test
    public void shouldReturnArgAsOptional() {
        final RequestMessage msg = RequestMessage.build("op").add("test", "testing").create();
        assertEquals("testing", msg.optionalArgs("test").get());
    }
}
