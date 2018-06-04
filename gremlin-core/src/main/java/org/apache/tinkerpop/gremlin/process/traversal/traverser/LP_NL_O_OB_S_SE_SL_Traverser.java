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

public class LP_NL_O_OB_S_SE_SL_Traverser<T> extends LP_O_OB_S_SE_SL_Traverser<T> {

    protected Stack<LabelledCounter> nestedLoops;

    protected LP_NL_O_OB_S_SE_SL_Traverser() {
    }

    public LP_NL_O_OB_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        this.nestedLoops = new Stack<>();
    }

    /////////////////

    @Override
    public int loops() {
        return this.nestedLoops.size() > 0 ? this.nestedLoops.peek().count() : 0;
    }

    @Override
    public void incrLoops(final String stepLabel) {
        // If we encounter a new stack label then grow the stack otherwise increment the loop count
        if (this.nestedLoops.size() == 0 || !this.nestedLoops.peek().hasLabel(stepLabel)) {
            this.nestedLoops.add(new LabelledCounter(stepLabel, (short)1));
        }
        else {
            this.nestedLoops.peek().increment();
        }
    }

    @Override
    public void resetLoops() {
        // Protect against reset without increment during RepeatStep setup
        if (this.nestedLoops.size() > 0) {
            this.nestedLoops.pop();
        }
    }

    /////////////////

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        final LP_NL_O_OB_S_SE_SL_Traverser<R> clone = (LP_NL_O_OB_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.nestedLoops = (Stack<LabelledCounter>)this.nestedLoops.clone();
        return clone;
    }

    @Override
    public Admin<T> split() {
        final LP_NL_O_OB_S_SE_SL_Traverser<T> clone = (LP_NL_O_OB_S_SE_SL_Traverser<T>) super.split();
        clone.nestedLoops = (Stack<LabelledCounter>)this.nestedLoops.clone();
        return clone;
    }

    @Override
    public void merge(final Admin<?> other) {
        super.merge(other);
    }

    /////////////////

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LP_NL_O_OB_S_SE_SL_Traverser)) return false;
        if (!super.equals(o)) return false;

        LP_NL_O_OB_S_SE_SL_Traverser<?> that = (LP_NL_O_OB_S_SE_SL_Traverser<?>) o;

        return nestedLoops.equals(that.nestedLoops);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + nestedLoops.hashCode();
        return result;
    }
}
