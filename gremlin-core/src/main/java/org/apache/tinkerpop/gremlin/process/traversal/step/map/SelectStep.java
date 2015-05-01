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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements TraversalParent {

    protected TraversalRing<Object, Object> traversalRing = new TraversalRing<>();
    private final List<String> selectLabels;

    public SelectStep(final Traversal.Admin traversal, final String... selectLabels) {
        super(traversal);
        this.selectLabels = Arrays.asList(selectLabels);
    }

    @Override
    protected Map<String, E> map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        final Map<String, E> bindings = new LinkedHashMap<>();

        if (start instanceof Map) {
            if (this.selectLabels.isEmpty())
                ((Map<String, Object>) start).forEach((key, value) -> bindings.put(key, (E) TraversalUtil.apply(value, this.traversalRing.next())));
            else
                this.selectLabels.forEach(label -> bindings.put(label, (E) TraversalUtil.apply(((Map) start).get(label), this.traversalRing.next())));
        } else {
            final Path path = traverser.path();
            if (this.selectLabels.isEmpty())
                path.labels().stream().flatMap(Set::stream).distinct().forEach(label -> bindings.put(label, (E) TraversalUtil.apply(path.<Object>get(label), this.traversalRing.next())));
            else
                this.selectLabels.forEach(label -> bindings.put(label, (E) TraversalUtil.apply(path.<Object>get(label), this.traversalRing.next())));
        }

        this.traversalRing.reset();
        return bindings;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabels, this.traversalRing);
    }

    @Override
    public SelectStep<S, E> clone() {
        final SelectStep<S, E> clone = (SelectStep<S, E>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone()));
        }
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.PATH);
    }
}
