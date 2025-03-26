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

import org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class NL_O_OB_S_SE_SL_Traverser<T> extends O_OB_S_SE_SL_Traverser<T> {

    protected Stack<LabelledCounter> nestedLoops;
    protected ReferenceMap<String,Object> loopNames;

    protected NL_O_OB_S_SE_SL_Traverser() {
    }

    public NL_O_OB_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        this.nestedLoops = new Stack<>();
        this.loopNames = new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
                AbstractReferenceMap.ReferenceStrength.WEAK);
    }

    /////////////////

    @Override
    public int loops() {
        return this.nestedLoops.peek().count();
    }

    @Override
    public int loops(final String loopName) {
        if (loopName == null)
            return loops();
        else if (this.loopNames.containsKey(loopName))
            return ((LabelledCounter) this.loopNames.get(loopName)).count();
        else
            throw new IllegalArgumentException("Loop name not defined: " + loopName);
    }

    @Override
    public void initialiseLoops(final String stepLabel, final String loopName) {
        if (this.nestedLoops.empty() || !this.nestedLoops.peek().hasLabel(stepLabel)) {
            final LabelledCounter lc = new LabelledCounter(stepLabel, (short) 0);
            this.nestedLoops.push(lc);
            if (loopName != null)
                this.loopNames.put(loopName, lc);
        }
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
        final NL_O_OB_S_SE_SL_Traverser<R> clone = (NL_O_OB_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.nestedLoops = new Stack<>();
        for(LabelledCounter lc : this.nestedLoops)
            clone.nestedLoops.push((LabelledCounter) lc.clone());

        if (this.loopNames != null) {
            clone.loopNames = new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
                    AbstractReferenceMap.ReferenceStrength.WEAK);

            final Iterator<Map.Entry<String,Object>> loopNamesIterator = this.loopNames.entrySet().iterator();
            while (loopNamesIterator.hasNext()) {
                final ReferenceMap.Entry<String,Object> pair = loopNamesIterator.next();

                final int idx = this.nestedLoops.indexOf(pair.getValue());
                if (idx != -1)
                    clone.loopNames.put(pair.getKey(), clone.nestedLoops.get(idx));
            }
        }

        return clone;
    }

    @Override
    public Admin<T> split() {
        final NL_O_OB_S_SE_SL_Traverser<T> clone = (NL_O_OB_S_SE_SL_Traverser<T>) super.split();
        clone.nestedLoops = new Stack<>();
        for(LabelledCounter lc : this.nestedLoops)
            clone.nestedLoops.push((LabelledCounter) lc.clone());

        if (this.loopNames != null) {
            clone.loopNames = new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
                    AbstractReferenceMap.ReferenceStrength.WEAK);

            final Iterator<Map.Entry<String,Object>> loopNamesIterator = this.loopNames.entrySet().iterator();
            while (loopNamesIterator.hasNext()) {
                final ReferenceMap.Entry<String,Object> pair = loopNamesIterator.next();

                final int idx = this.nestedLoops.indexOf(pair.getValue());
                if (idx != -1)
                    clone.loopNames.put(pair.getKey(), clone.nestedLoops.get(idx));
            }
        }

        return clone;
    }

    @Override
    public void merge(final Admin<?> other) {
        super.merge(other);
    }

    @Override
    public Set<String> getLoopNames() {
        return loopNames.keySet();
    }

    /////////////////

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof NL_O_OB_S_SE_SL_Traverser)) return false;
        if (!super.equals(o)) return false;

        final NL_O_OB_S_SE_SL_Traverser<?> that = (NL_O_OB_S_SE_SL_Traverser<?>) o;

        if (!this.nestedLoops.equals(that.nestedLoops)) return false;
        return this.loopNames != null ? this.loopNames.equals(that.loopNames) : that.loopNames == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.nestedLoops.hashCode();
        result = 31 * result + (this.loopNames != null ? this.loopNames.hashCode() : 0);

        return result;
    }

}
