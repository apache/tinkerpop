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
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ProjectStep<S, E> extends MapStep<S, Map<String, E>> implements TraversalParent, ByModulating {

    private final List<String> projectKeys;
    private TraversalRing<S, E> traversalRing;

    public ProjectStep(final Traversal.Admin traversal, final String... projectKeys) {
        super(traversal);
        this.projectKeys = Arrays.asList(projectKeys);
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    protected Map<String, E> map(final Traverser.Admin<S> traverser) {
        final Map<String, E> end = new LinkedHashMap<>(this.projectKeys.size(), 1.0f);
        for (final String projectKey : this.projectKeys) {
            end.put(projectKey, TraversalUtil.applyNullable(traverser, this.traversalRing.next()));
        }
        this.traversalRing.reset();
        return end;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.projectKeys, this.traversalRing);
    }

    @Override
    public ProjectStep<S, E> clone() {
        final ProjectStep<S, E> clone = (ProjectStep<S, E>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.traversalRing.hashCode() ^ this.projectKeys.hashCode();
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        int i = 0;
        for (final Traversal.Admin<?, ?> traversal : this.traversalRing.getTraversals()) {
            if (null != traversal && traversal.equals(oldTraversal)) {
                this.traversalRing.setTraversal(i, this.integrateChild(newTraversal));
                break;
            }
            i++;
        }
    }

    public List<String> getProjectKeys() {
        return this.projectKeys;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }
}
