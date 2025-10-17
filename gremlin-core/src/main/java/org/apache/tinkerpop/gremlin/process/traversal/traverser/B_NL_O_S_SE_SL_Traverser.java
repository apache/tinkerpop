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

import java.util.Objects;
import java.util.Stack;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;

public class B_NL_O_S_SE_SL_Traverser<T> extends B_O_S_SE_SL_Traverser<T> implements NL_SL_Traverser<T> {

    protected Stack<LabelledCounter> nestedLoops;
    protected ReferenceMap loopNames = null;

    protected B_NL_O_S_SE_SL_Traverser() {
    }

    public B_NL_O_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        this.nestedLoops = new Stack<>();
        this.loopNames = new ReferenceMap(ReferenceMap.HARD, ReferenceMap.WEAK);
    }

    @Override
    public Stack<LabelledCounter> getNestedLoops() {
        return nestedLoops;
    }

    @Override
    public ReferenceMap getNestedLoopNames() {
        return loopNames;
    }
    
    @Override
    public TraverserRequirement getLoopRequirement() {
        return TraverserRequirement.NESTED_LOOP;
    }

    /////////////////

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        final B_NL_O_S_SE_SL_Traverser<R> clone = (B_NL_O_S_SE_SL_Traverser<R>) super.split(r, step);
        cloneLoopState(clone);
        return clone;
    }

    @Override
    public Admin<T> split() {
        final B_NL_O_S_SE_SL_Traverser<T> clone = (B_NL_O_S_SE_SL_Traverser<T>) super.split();
        cloneLoopState(clone);
        return clone;
    }

    @Override
    public void merge(final Admin<?> other) {
        super.merge(other);
    }

    /////////////////

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof B_NL_O_S_SE_SL_Traverser)) return false;
        if (!super.equals(o)) return false;

        final B_NL_O_S_SE_SL_Traverser<?> that = (B_NL_O_S_SE_SL_Traverser<?>) o;

        if (!this.nestedLoops.equals(that.nestedLoops)) return false;
        return Objects.equals(this.loopNames, that.loopNames);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.nestedLoops.hashCode();
        result = 31 * result + (this.loopNames != null ? this.loopNames.hashCode() : 0);

        return result;
    }

    private void cloneLoopState(final B_NL_O_S_SE_SL_Traverser<?> clone) {
        clone.nestedLoops = new Stack<>();
        clone.loopNames = new ReferenceMap(ReferenceMap.HARD, ReferenceMap.WEAK);
        copyNestedLoops(clone.nestedLoops, clone.loopNames);
    }

}
