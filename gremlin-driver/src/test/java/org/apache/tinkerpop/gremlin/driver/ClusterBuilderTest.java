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
                {"maxInProcessPerConnectionNeg1", Cluster.build().maxInProcessPerConnection(-1), "maxInProcessPerConnection must be greater than equal to zero"},
                {"maxSimultaneousUsagePerConnectionNeg1", Cluster.build().maxSimultaneousUsagePerConnection(-1), "maxSimultaneousUsagePerConnection must be greater than equal to zero"},
                {"maxConnectionPoolSize0", Cluster.build().maxConnectionPoolSize(0), "maxConnectionPoolSize must be greater than zero"},
                {"maxConnectionPoolSizeNeg1", Cluster.build().maxConnectionPoolSize(-1), "maxConnectionPoolSize must be greater than zero"},
                {"maxConnectionPoolSize0", Cluster.build().maxWaitForConnection(0), "maxWaitForConnection must be greater than zero"},
                {"maxWaitForSessionClose0", Cluster.build().maxWaitForSessionClose(0), "maxWaitForSessionClose must be greater than zero"},
                {"maxWaitForSessionCloseNeg1", Cluster.build().maxWaitForSessionClose(-1), "maxWaitForSessionClose must be greater than zero"},
                {"maxContentLength0", Cluster.build().maxContentLength(0), "maxContentLength must be greater than zero"},
                {"maxContentLengthNeg1", Cluster.build().maxContentLength(-1), "maxContentLength must be greater than zero"},
                {"reconnectInterval0", Cluster.build().reconnectInterval(0), "reconnectInterval must be greater than zero"},
                {"reconnectIntervalNeg1", Cluster.build().reconnectInterval(-1), "reconnectInterval must be greater than zero"},
                {"resultIterationBatchSize0", Cluster.build().resultIterationBatchSize(0), "resultIterationBatchSize must be greater than zero"},
                {"resultIterationBatchSizeNeg1", Cluster.build().resultIterationBatchSize(-1), "resultIterationBatchSize must be greater than zero"},
                {"nioPoolSize0", Cluster.build().nioPoolSize(0), "nioPoolSize must be greater than zero"},
                {"nioPoolSizeNeg1", Cluster.build().nioPoolSize(-1), "nioPoolSize must be greater than zero"},
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
