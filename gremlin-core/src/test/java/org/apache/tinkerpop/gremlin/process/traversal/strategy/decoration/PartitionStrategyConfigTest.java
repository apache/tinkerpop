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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration2.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PartitionStrategyConfigTest {

    @Parameterized.Parameter(0)
    public PartitionStrategy expectedStrategy;

    @Parameterized.Parameters(name = "expectedStrategy={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {PartitionStrategy.build().partitionKey("partitionKey1").writePartition("writePartition1").create()},
                {PartitionStrategy.build().partitionKey("partitionKey2").writePartition("writePartition2").readPartitions("readPartition1", "readPartition2").create()},
                {PartitionStrategy.build().partitionKey("partitionKey2").writePartition("writePartition2").readPartitions("readPartition1").create()},
        });
    }

    @Test
    public void shouldRoundTripConfiguration() {
        // Get the configuration from the expected strategy
        final Configuration configuration = expectedStrategy.getConfiguration();

        // Recreate the strategy from the configuration
        final PartitionStrategy recreatedStrategy = PartitionStrategy.create(configuration);

        // Assert that the recreated strategy matches the expected strategy
        assertEquals(expectedStrategy.getPartitionKey(), recreatedStrategy.getPartitionKey());
        assertEquals(expectedStrategy.getWritePartition(), recreatedStrategy.getWritePartition());
        assertEquals(expectedStrategy.getReadPartitions(), recreatedStrategy.getReadPartitions());
    }
}