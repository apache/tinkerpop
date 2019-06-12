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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TraversalExplanationSerializer extends SimpleTypeSerializer<TraversalExplanation> implements TransformSerializer<TraversalExplanation> {
    private static final String ORIGINAL = "original";
    private static final String FINAL = "final";
    private static final String INTERMEDIATE = "intermediate";
    private static final String CATEGORY = "category";
    private static final String TRAVERSAL = "traversal";
    private static final String STRATEGY = "strategy";

    public TraversalExplanationSerializer() {
        super(null);
    }

    @Override
    protected TraversalExplanation readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        throw new SerializationException("A TraversalExplanation should not be read individually");
    }

    @Override
    protected void writeValue(final TraversalExplanation value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        throw new SerializationException("A TraversalExplanation should not be written individually");
    }

    /**
     * Creates a Map containing "original", "intermediate" and "final" keys.
     */
    @Override
    public Object transform(final TraversalExplanation value) {
        final Map<String, Object> result = new HashMap<>();
        result.put(ORIGINAL, getTraversalSteps(value.getOriginalTraversal()));

        final List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> strategyTraversals = value.getStrategyTraversals();

        result.put(INTERMEDIATE,
                strategyTraversals.stream().map(pair -> {
                    final Map<String, Object> item = new HashMap<>();
                    item.put(STRATEGY, pair.getValue0().toString());
                    item.put(CATEGORY, pair.getValue0().getTraversalCategory().getSimpleName());
                    item.put(TRAVERSAL, getTraversalSteps(pair.getValue1()));
                    return item;
                }).collect(Collectors.toList()));

        result.put(FINAL, getTraversalSteps(strategyTraversals.isEmpty()
                ? value.getOriginalTraversal() : strategyTraversals.get(strategyTraversals.size() - 1).getValue1()));
        return result;
    }

    private static List<String> getTraversalSteps(final Traversal.Admin<?, ?> t) {
        return t.getSteps().stream().map(Object::toString).collect(Collectors.toList());
    }
}
