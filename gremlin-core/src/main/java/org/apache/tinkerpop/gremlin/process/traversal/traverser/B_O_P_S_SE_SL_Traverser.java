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
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_P_S_SE_SL_Traverser<T> extends B_O_S_SE_SL_Traverser<T> {

    protected Path path;

    protected B_O_P_S_SE_SL_Traverser() {
    }

    public B_O_P_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        final Optional<String> stepLabel = step.getLabel();
        this.path = stepLabel.isPresent() ?
                ImmutablePath.make().extend(t, stepLabel.get()) :
                ImmutablePath.make().extend(t);
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
        this.path = DetachedFactory.detach(this.path, true);
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        if (this.t instanceof Attachable && !(((Attachable) this.t).get() instanceof Path))
            this.t = ((Attachable<T>) this.t).attach(method);
        return this.t;
    }

    /////////////////

    @Override
    public void merge(final Traverser.Admin<?> other) {
        this.bulk = this.bulk + other.bulk();
    }

    @Override
    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {

            final B_O_P_S_SE_SL_Traverser<R> clone = (B_O_P_S_SE_SL_Traverser<R>) super.split(r,step);
            final Optional<String> stepLabel = step.getLabel();
            clone.path = stepLabel.isPresent() ? clone.path.clone().extend(r, stepLabel.get()) : clone.path.clone().extend(r);
            //clone.sack = null == clone.sack ? null : clone.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(clone.sack);
            return clone;

    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof B_O_P_S_SE_SL_Traverser)
                && ((B_O_P_S_SE_SL_Traverser) object).path().equals(this.path) // TODO: path equality
                && ((B_O_P_S_SE_SL_Traverser) object).get().equals(this.t)
                && ((B_O_P_S_SE_SL_Traverser) object).getStepId().equals(this.getStepId())
                && ((B_O_P_S_SE_SL_Traverser) object).loops() == this.loops()
                && (null == this.sack);
    }

}
