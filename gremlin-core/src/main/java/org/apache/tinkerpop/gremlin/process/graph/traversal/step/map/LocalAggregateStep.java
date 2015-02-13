/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class LocalAggregateStep<S, E> extends MapStep<S, E> {

    public LocalAggregateStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    protected <E2> Collection<E2> collect(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Collection)
            return (Collection<E2>) start;
        else if (start instanceof Map)
            return (Collection<E2>) ((Map) start).values();
        else
            return Collections.singleton((E2) start);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
