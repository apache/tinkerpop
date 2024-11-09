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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_LP_O_P_S_SE_SL_Traverser<T> extends B_O_S_SE_SL_Traverser<T> {

    protected Path path;

    protected B_LP_O_P_S_SE_SL_Traverser() {
    }

    public B_LP_O_P_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        this.path = ImmutablePath.make().extend(t, step.getLabels());
    }

    /////////////////

    @Override
    public Path path() {
        return this.path;
    }

    /////////////////

    @Override
    public Traverser.Admin<T> detach() {
        super.detach();
        this.path = ReferenceFactory.detach(this.path);
        return this;
    }

    /////////////////

    @Override
    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {
        final B_LP_O_P_S_SE_SL_Traverser<R> clone = (B_LP_O_P_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.path = clone.path.clone().extend(r, step.getLabels());
        return clone;
    }

    @Override
    public Traverser.Admin<T> split() {
        final B_LP_O_P_S_SE_SL_Traverser<T> clone = (B_LP_O_P_S_SE_SL_Traverser<T>) super.split();
        clone.path = clone.path.clone();
        return clone;
    }

    @Override
    public void dropPath() {
        path = ImmutablePath.make();
    }

    @Override
    public void addLabels(final Set<String> labels) {
        this.path = this.path.extend(labels);
    }

    @Override
    public int hashCode() {
        return carriesUnmergeableSack() ? System.identityHashCode(this) : (31 * super.hashCode() + this.path.hashCode());
    }

    protected final boolean equals(final B_LP_O_P_S_SE_SL_Traverser other) {
        return super.equals(other) && other.path.equals(this.path);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof B_LP_O_P_S_SE_SL_Traverser && this.equals((B_LP_O_P_S_SE_SL_Traverser) object);
    }
}
