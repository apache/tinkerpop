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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.driver.AbstractResultQueueTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteTraversalSideEffectsTest extends AbstractResultQueueTest {

    @Test
    public void shouldNotContactRemoteForKeysAfterCloseIsCalled() throws Exception {
        final Client client = mock(Client.class);
        mockClientForCall(client);
        mockClientForCall(client);

        final UUID sideEffectKey = UUID.fromString("31dec2c6-b214-4a6f-a68b-996608dce0d9");
        final TraversalSideEffects sideEffects = new DriverRemoteTraversalSideEffects(client, sideEffectKey, null);

        assertEquals(1, sideEffects.keys().size());
        sideEffects.close();

        // call this again and again and it will only hit the cached keys - no more server calls
        assertEquals(1, sideEffects.keys().size());
        assertEquals(1, sideEffects.keys().size());
        assertEquals(1, sideEffects.keys().size());
        assertEquals(1, sideEffects.keys().size());

        // once for the keys and once for the close message
        verify(client, times(2)).submitAsync(any(RequestMessage.class));
    }

    @Test
    public void shouldNotContactRemoteMoreThanOnceForClose() throws Exception {
        final Client client = mock(Client.class);
        mockClientForCall(client);
        mockClientForCall(client);

        final UUID sideEffectKey = UUID.fromString("31dec2c6-b214-4a6f-a68b-996608dce0d9");
        final TraversalSideEffects sideEffects = new DriverRemoteTraversalSideEffects(client, sideEffectKey, null);

        sideEffects.close();
        sideEffects.close();
        sideEffects.close();
        sideEffects.close();
        sideEffects.close();

        assertEquals(0, sideEffects.keys().size());

        // once for the keys and once for the close message
        verify(client, times(1)).submitAsync(any(RequestMessage.class));
    }

    private void mockClientForCall(final Client client) throws Exception {
        final ResultSet returnedResultSet = new ResultSet(resultQueue, pool, readCompleted, RequestMessage.build("traversal").create(), null);
        addToQueue(1, 0, true, true, 1);
        final CompletableFuture<ResultSet> returnedFuture = new CompletableFuture<>();
        returnedFuture.complete(returnedResultSet);

        // the return is just generic garbage from addToQueue for any call to submitAsync() - but given the logic
        // of DriverRemoteTraversalSideEffects, that's ok
        when(client.submitAsync(any(RequestMessage.class))).thenReturn(returnedFuture);
    }
}
