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
package org.apache.tinkerpop.gremlin.process.traversal.traverser;


import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractPathTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.SparsePath;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_PA_S_SE_SL_Traverser<T> extends AbstractPathTraverser<T> {

    protected B_O_PA_S_SE_SL_Traverser() {
    }

    public B_O_PA_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        final Optional<String> stepLabel = step.getLabel();
        this.path = stepLabel.isPresent() ?
                getOrCreateFromCache(this.sideEffects).extend(t, stepLabel.get()) :
                getOrCreateFromCache(this.sideEffects).extend(t);
    }

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.future.hashCode() + this.loops;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof B_O_PA_S_SE_SL_Traverser
                && ((B_O_PA_S_SE_SL_Traverser) object).get().equals(this.t)
                && ((B_O_PA_S_SE_SL_Traverser) object).getStepId().equals(this.getStepId())
                && ((B_O_PA_S_SE_SL_Traverser) object).loops() == this.loops()
                && (null == this.sack);
    }

    @Override
    public Traverser.Admin<T> attach(final Vertex vertex) {
        super.attach(vertex);
        final Path newSparsePath = getOrCreateFromCache(this.sideEffects);
        this.path.forEach((object, labels) -> newSparsePath.extend(object, labels.toArray(new String[labels.size()])));
        this.path = newSparsePath;
        return this;
    }

    //////////////////////

    private static final Map<TraversalSideEffects, SparsePath> PATH_CACHE = new WeakHashMap<>();

    private static SparsePath getOrCreateFromCache(final TraversalSideEffects sideEffects) {
        SparsePath path = PATH_CACHE.get(sideEffects);
        if (null == path) {
            path = SparsePath.make();
            PATH_CACHE.put(sideEffects, path);
        }
        return path;
    }

}
