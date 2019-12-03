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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IndexStep<S, E> extends ScalarMapStep<S, E> implements TraversalParent, Configuring {

    private final static IllegalArgumentException INVALID_CONFIGURATION_EXCEPTION =
            new IllegalArgumentException("WithOptions.indexer requires a single Integer argument (possible " + "" +
                    "values are: WithOptions.[list|map])");

    private final Parameters parameters = new Parameters();

    private Function<Iterator, Object> indexer;

    public IndexStep(final Traversal.Admin traversal) {
        super(traversal);
        this.configure(WithOptions.indexer, WithOptions.list);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        //noinspection unchecked
        return (E) indexer.apply(IteratorUtils.asIterator(traverser.get()));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.indexer.hashCode();
    }

    private static List<List<Object>> indexedList(final Iterator iterator) {
        final List<List<Object>> list = new ArrayList<>();
        int i = 0;
        while (iterator.hasNext()) {
            list.add(Arrays.asList(iterator.next(), i++));
        }
        return Collections.unmodifiableList(list);
    }

    private static Map<Integer, Object> indexedMap(final Iterator iterator) {
        final Map<Integer, Object> map = new LinkedHashMap<>();
        int i = 0;
        while (iterator.hasNext()) {
            map.put(i++, iterator.next());
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void configure(final Object... keyValues) {
        final Integer indexer;
        if (keyValues[0].equals(WithOptions.indexer)) {
            if (keyValues.length == 2 && keyValues[1] instanceof Integer) {
                indexer = (Integer) keyValues[1];
            } else {
                throw INVALID_CONFIGURATION_EXCEPTION;
            }
            if (indexer == WithOptions.list) {
                this.indexer = IndexStep::indexedList;
            } else if (indexer == WithOptions.map) {
                this.indexer = IndexStep::indexedMap;
            } else {
                throw INVALID_CONFIGURATION_EXCEPTION;
            }
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }
}

