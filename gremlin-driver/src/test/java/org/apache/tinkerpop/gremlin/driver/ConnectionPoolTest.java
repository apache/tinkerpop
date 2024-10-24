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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectionPoolTest {

    @Test
    public void shouldCreateAndScalePool() throws TimeoutException {
        final AtomicInteger connectionsCreated = new AtomicInteger(0);

        final Connection mockConn0 = mock(Connection.class);
        when(mockConn0.isBorrowed()).thenReturn(new AtomicBoolean(false));
        final Connection mockConn1 = mock(Connection.class);
        when(mockConn1.isBorrowed()).thenReturn(new AtomicBoolean(false));
        final List<Connection> mockConns = Arrays.asList(mockConn0, mockConn1);

        final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        when(connectionFactory.create(any(ConnectionPool.class)))
                .thenAnswer(i -> mockConns.get(connectionsCreated.getAndIncrement()));

        final Cluster cluster = mock(Cluster.class);
        when(cluster.connectionPoolSettings()).thenReturn(new Settings.ConnectionPoolSettings());
        final ScheduledThreadPoolExecutor connectionScheduler = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern("gremlin-driver-conn-scheduler-%d").build());
        when(cluster.connectionScheduler()).thenReturn(connectionScheduler);

        final Host host = mock(Host.class);

        final Client client = new Client.ClusteredClient(cluster);

        // create pool - starts with 1 connection
        final ConnectionPool connectionPool = new ConnectionPool(host, client, Optional.of(2), connectionFactory);
        // try to borrow a connection.
        final Connection conn0 = connectionPool.borrowConnection(100, TimeUnit.MILLISECONDS);

        assertNotNull(connectionPool);
        assertNotNull(conn0);
        assertEquals(1, connectionsCreated.get());

        // try to borrow connection. conn0 is mocked as borrowed, so should create new one
        when(mockConn0.isBorrowed()).thenReturn(new AtomicBoolean(true));
        final Connection conn1 = connectionPool.borrowConnection(100, TimeUnit.MILLISECONDS);

        assertNotNull(conn1);
        assertEquals(2, connectionsCreated.get());

        // mark conn1 as borrowed and try to get one more connection
        when(mockConn1.isBorrowed()).thenReturn(new AtomicBoolean(true));
        try {
            connectionPool.borrowConnection(1000, TimeUnit.MILLISECONDS);
            fail("Pool already at fool capacity, connection can't be added");
        } catch (TimeoutException te) {
            assertEquals(2, connectionsCreated.get());
        }

        // return conn0 to pool, can be borrowed again
        connectionPool.returnConnection(conn0);
        when(mockConn0.isBorrowed()).thenReturn(new AtomicBoolean(false));
        final Connection conn00 = connectionPool.borrowConnection(100, TimeUnit.MILLISECONDS);

        assertNotNull(conn00);
        assertEquals(2, connectionsCreated.get());
    }
}
