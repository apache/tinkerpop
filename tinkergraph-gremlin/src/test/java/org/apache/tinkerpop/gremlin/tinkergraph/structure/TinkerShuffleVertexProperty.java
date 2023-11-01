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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerShuffleGraph.shuffleIterator;

/**
 * @author Cole Greer (https://github.com/Cole-Greer)
 */
public class TinkerShuffleVertexProperty<V> extends TinkerVertexProperty<V> {


    /**
     * This constructor will not validate the ID type against the {@link Graph}.  It will always just use a
     * {@code Long} for its identifier.  This is useful for constructing a {@link VertexProperty} for usage
     * with {@link TinkerGraphComputerView}.
     */
    public TinkerShuffleVertexProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(vertex, key, value, propertyKeyValues);
    }

    /**
     * Use this constructor to construct {@link VertexProperty} instances for {@link TinkerGraph} where the {@code id}
     * can be explicitly set and validated against the expected data type.
     */
    public TinkerShuffleVertexProperty(final Object id, final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, vertex, key, value, propertyKeyValues);
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return shuffleIterator(super.properties(propertyKeys));
    }
}
