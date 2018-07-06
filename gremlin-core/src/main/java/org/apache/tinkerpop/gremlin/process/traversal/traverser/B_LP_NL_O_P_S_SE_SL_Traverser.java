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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;

import java.util.Stack;

public class B_LP_NL_O_P_S_SE_SL_Traverser<T> extends B_LP_O_P_S_SE_SL_Traverser<T> {

    protected Stack<LabelledCounter> nestedLoops;

    protected B_LP_NL_O_P_S_SE_SL_Traverser() {
    }

    public B_LP_NL_O_P_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        this.nestedLoops = new Stack<>();
    }

    /////////////////

    @Override
    public int loops() {
        return this.nestedLoops.peek().count();
    }

    @Override
    public void initialiseLoops(final String stepLabel) {
        if (this.nestedLoops.empty() || !this.nestedLoops.peek().hasLabel(stepLabel))
            this.nestedLoops.push(new LabelledCounter(stepLabel, (short)0));
    }

    @Override
    public void incrLoops() {
        this.nestedLoops.peek().increment();
    }

    @Override
    public void resetLoops() {
        this.nestedLoops.pop();
    }

    /////////////////

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        final B_LP_NL_O_P_S_SE_SL_Traverser<R> clone = (B_LP_NL_O_P_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.nestedLoops = new Stack<>();
        for(LabelledCounter lc : this.nestedLoops)
            clone.nestedLoops.push((LabelledCounter) lc.clone());
        return clone;
    }

    @Override
    public Admin<T> split() {
        final B_LP_NL_O_P_S_SE_SL_Traverser<T> clone = (B_LP_NL_O_P_S_SE_SL_Traverser<T>) super.split();
        clone.nestedLoops = new Stack<>();
        for(LabelledCounter lc : this.nestedLoops)
            clone.nestedLoops.push((LabelledCounter) lc.clone());
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
        if (!(o instanceof B_LP_NL_O_P_S_SE_SL_Traverser)) return false;
        if (!super.equals(o)) return false;

        B_LP_NL_O_P_S_SE_SL_Traverser<?> that = (B_LP_NL_O_P_S_SE_SL_Traverser<?>) o;

        return this.nestedLoops.equals(that.nestedLoops);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.nestedLoops.hashCode();
        return result;
    }
}
