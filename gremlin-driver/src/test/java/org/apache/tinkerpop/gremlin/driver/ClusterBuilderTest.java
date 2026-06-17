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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ClusterBuilderTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"maxConnections0", Cluster.build().maxConnections(0), "maxConnections must be greater than zero"},
                {"maxConnectionsNeg1", Cluster.build().maxConnections(-1), "maxConnections must be greater than zero"},
                {"maxConnectionPoolSize0Deprecated", Cluster.build().maxConnectionPoolSize(0), "maxConnections must be greater than zero"},
                {"maxWaitForConnection0", Cluster.build().maxWaitForConnection(0), "maxWaitForConnection must be greater than zero"},
                {"maxWaitForClose0", Cluster.build().maxWaitForClose(0), "maxWaitForClose must be greater than zero"},
                {"maxWaitForCloseNeg1", Cluster.build().maxWaitForClose(-1), "maxWaitForClose must be greater than zero"},
                {"maxResponseHeaderBytes0", Cluster.build().maxResponseHeaderBytes(0), "maxResponseHeaderBytes must be greater than zero"},
                {"maxResponseHeaderBytesNeg1", Cluster.build().maxResponseHeaderBytes(-1), "maxResponseHeaderBytes must be greater than zero"},
                {"reconnectInterval0", Cluster.build().reconnectInterval(0), "reconnectInterval must be greater than zero"},
                {"reconnectIntervalNeg1", Cluster.build().reconnectInterval(-1), "reconnectInterval must be greater than zero"},
                {"defaultBatchSize0", Cluster.build().defaultBatchSize(0), "defaultBatchSize must be greater than zero"},
                {"defaultBatchSizeNeg1", Cluster.build().defaultBatchSize(-1), "defaultBatchSize must be greater than zero"},
                {"readTimeoutNeg1", Cluster.build().readTimeout(-1), "readTimeout must be greater than or equal to zero"},
                {"keepAliveTimeNeg1", Cluster.build().keepAliveTime(-1), "keepAliveTime must be greater than or equal to zero"},
                {"nioPoolSize0", Cluster.build().nioPoolSize(0), "nioPoolSize must be greater than zero"},
                {"nioPoolSizeNeg1", Cluster.build().nioPoolSize(-1), "nioPoolSize must be greater than zero"},
                {"connectTimeout0", Cluster.build().connectTimeout(0), "connectTimeout must be greater than zero"},
                {"idleTimeoutNeg1", Cluster.build().idleTimeout(-1), "idleTimeout must be zero or greater than or equal to 1000"},
                {"idleTimeoutOne", Cluster.build().idleTimeout(1), "idleTimeout must be zero or greater than or equal to 1000"},
                {"idleTimeout999", Cluster.build().idleTimeout(999), "idleTimeout must be zero or greater than or equal to 1000"},
                {"workerPoolSize0", Cluster.build().workerPoolSize(0), "workerPoolSize must be greater than zero"},
                {"workerPoolSizeNeg1", Cluster.build().workerPoolSize(-1), "workerPoolSize must be greater than zero"}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Cluster.Builder builder;

    @Parameterized.Parameter(value = 2)
    public String expectedErrorMessage;

    @Test
    public void shouldNotConstructAnInvalidConnection() {
        try {
            builder.create();
            fail("Should not have created cluster instance");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalArgumentException.class));
            assertEquals(expectedErrorMessage, ex.getMessage());
        }
    }
}
