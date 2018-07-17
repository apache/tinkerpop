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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class O_OB_S_SE_SL_Traverser<T> extends O_Traverser<T> {

    protected Object sack = null;
    protected short loops = 0;  // an optimization hack to use a short internally to save bits :)
    protected String loopName = null;
    protected transient TraversalSideEffects sideEffects;
    protected String future = HALT;
    protected long bulk = 1L;

    protected O_OB_S_SE_SL_Traverser() {
    }

    public O_OB_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t);
        this.sideEffects = step.getTraversal().getSideEffects();
        if (null != this.sideEffects.getSackInitialValue())
            this.sack = this.sideEffects.getSackInitialValue().get();
    }

    /////////////////

    @Override
    public void setBulk(final long count) {
        this.bulk = count > 0 ? 1L : 0L;
    }

    @Override
    public long bulk() {
        return this.bulk;
    }

    @Override
    public <S> S sack() {
        return (S) this.sack;
    }

    @Override
    public <S> void sack(final S object) {
        this.sack = object;
    }

    /////////////////

    @Override
    public int loops() {
        return this.loops;
    }

    @Override
    public int loops(final String loopName) {
        if (loopName == null || this.loopName != null && this.loopName.equals(loopName))
            return this.loops;
        else
            throw new IllegalArgumentException("Loop name not defined: " + loopName);
    }

    @Override
    public void initialiseLoops(final String stepLabel , final String loopName){
        this.loopName = loopName;
    }

    @Override
    public void incrLoops() {
        this.loops++;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    /////////////////

    @Override
    public String getStepId() {
        return this.future;
    }

    @Override
    public void setStepId(final String stepId) {
        this.future = stepId;
    }

    /////////////////

    @Override
    public TraversalSideEffects getSideEffects() {
        return this.sideEffects;
    }


    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    /////////////////

    @Override
    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {
        final O_OB_S_SE_SL_Traverser<R> clone = (O_OB_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.sack = null == clone.sack ? null : null == clone.sideEffects.getSackSplitter() ? clone.sack : clone.sideEffects.getSackSplitter().apply(clone.sack);
        return clone;
    }

    @Override
    public Traverser.Admin<T> split() {
        final O_OB_S_SE_SL_Traverser<T> clone = (O_OB_S_SE_SL_Traverser<T>) super.split();
        clone.sack = null == clone.sack ? null : null == clone.sideEffects.getSackSplitter() ? clone.sack : clone.sideEffects.getSackSplitter().apply(clone.sack);
        return clone;
    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        super.merge(other);
        if (null != this.sack && null != this.sideEffects.getSackMerger())
            this.sack = this.sideEffects.getSackMerger().apply(this.sack, other.sack());
    }

    /////////////////

    final boolean carriesUnmergeableSack() {
        // hmmm... serialization in OLAP destroys the transient sideEffects
        return null != this.sack && (null == this.sideEffects || null == this.sideEffects.getSackMerger());
    }

    @Override
    public int hashCode() {
        return carriesUnmergeableSack() ? System.identityHashCode(this) : (super.hashCode() ^ this.loops ^ Objects.hashCode(this.loopName));
    }

    protected  final boolean equals(final O_OB_S_SE_SL_Traverser other) {
        return super.equals(other) && other.loops == this.loops && other.future.equals(this.future)
                && (this.loopName != null ? this.loopName.equals(other.loopName) : other.loopName == null)
                && !carriesUnmergeableSack();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof O_OB_S_SE_SL_Traverser && this.equals((O_OB_S_SE_SL_Traverser) object);
    }
}
