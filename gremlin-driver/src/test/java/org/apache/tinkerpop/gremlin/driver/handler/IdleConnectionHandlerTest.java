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

package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdleConnectionHandlerTest {
    private EmbeddedChannel testChannel;

    @Before
    public void setup() {
        testChannel = new EmbeddedChannel(new IdleConnectionHandler());
    }

    @After
    public void teardown() {
        // if any exceptions happened, throw them otherwise the test will only silently fail
        testChannel.checkException();
    }


    @Test
    public void userEventTriggered_setsIdleStateEventAttribute() {
        testChannel.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        assertTrue(testChannel.hasAttr(IdleConnectionHandler.IDLE_STATE_EVENT));
    }

    @Test
    public void userEventTriggered_notIdleStateEvent_doesNotSetAttribute() {
        testChannel.pipeline().fireUserEventTriggered("some other event");
        assertFalse(testChannel.hasAttr(IdleConnectionHandler.IDLE_STATE_EVENT));
    }
}