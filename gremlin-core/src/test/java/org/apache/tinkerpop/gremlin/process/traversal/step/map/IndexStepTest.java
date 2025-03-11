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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IndexStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.index(),
                __.index().with(WithOptions.indexer, WithOptions.map)
        );
    }

    @Test
    public void testDefault() {
        assertEquals(__.index(), __.index().with(WithOptions.indexer, WithOptions.list));
    }

    @Test
    public void testListIndexerSerializationDeserializationRoundTrip() {
        // serialization and deserialization should not throw exception
        byte[] serialized = SerializationUtils.serialize(__.index().with(WithOptions.indexer, WithOptions.list));
        Object deserialized = SerializationUtils.deserialize(serialized);

        DefaultTraversal<Object, Object> gtr = (DefaultTraversal<Object, Object>) deserialized;
        assertEquals(1, gtr.getSteps().size());
        IndexStep<Object, Object> step = (IndexStep<Object, Object>) gtr.getSteps().get(0);
        assertEquals(IndexStep.IndexerType.LIST, step.getIndexerType());
        assertTrue(step.getIndexer() instanceof IndexStep.ListIndexFunction);
    }

    @Test
    public void testMapIndexerSerializationDeserializationRoundTrip() {
        // serialization and deserialization should not throw exception
        byte[] serialized = SerializationUtils.serialize(__.index().with(WithOptions.indexer, WithOptions.map));
        Object deserialized = SerializationUtils.deserialize(serialized);

        DefaultTraversal<Object, Object> gtr = (DefaultTraversal<Object, Object>) deserialized;
        assertEquals(1, gtr.getSteps().size());
        IndexStep<Object, Object> step = (IndexStep<Object, Object>) gtr.getSteps().get(0);
        assertEquals(IndexStep.IndexerType.MAP, step.getIndexerType());
        assertTrue(step.getIndexer() instanceof IndexStep.MapIndexFunction);
    }
}
