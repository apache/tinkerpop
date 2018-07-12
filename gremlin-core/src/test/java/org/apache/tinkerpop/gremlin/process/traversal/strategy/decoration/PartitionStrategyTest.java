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

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategyTest {

    @Test(expected = IllegalStateException.class)
    public void shouldNotConstructWithoutPartitionKey() {
        PartitionStrategy.build().create();
    }

    @Test
    public void shouldConstructPartitionStrategy() {
        final PartitionStrategy strategy = PartitionStrategy.build()
                .partitionKey("p").writePartition("a").readPartitions("a").create();
        assertEquals("a", strategy.getReadPartitions().iterator().next());
        assertEquals(1, strategy.getReadPartitions().size());
        assertEquals("p", strategy.getPartitionKey());
    }

    @Test
    public void shouldConstructPartitionStrategyWithMultipleReadPartitions() {
        final PartitionStrategy strategy = PartitionStrategy.build()
                .partitionKey("p").writePartition("a")
                .readPartitions("a")
                .readPartitions("b")
                .readPartitions("c").create();

        assertTrue(IteratorUtils.asList(strategy.getReadPartitions().iterator()).contains("a"));
        assertTrue(IteratorUtils.asList(strategy.getReadPartitions().iterator()).contains("b"));
        assertTrue(IteratorUtils.asList(strategy.getReadPartitions().iterator()).contains("c"));
        assertEquals(3, strategy.getReadPartitions().size());
        assertEquals("p", strategy.getPartitionKey());
    }
}
