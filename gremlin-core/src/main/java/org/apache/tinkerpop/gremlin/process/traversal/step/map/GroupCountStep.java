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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S, E> extends ReducingBarrierStep<S, Map<E, Long>> implements MapReducer, TraversalParent {

    private Traversal.Admin<S, E> groupTraversal = null;

    public GroupCountStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(HashMapSupplier.instance());
        this.setBiFunction(new GroupCountBiFunction(this));
    }


    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> groupTraversal) {
        this.groupTraversal = this.integrateChild(groupTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.groupTraversal ? Collections.emptyList() : Collections.singletonList(this.groupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    @Override
    public MapReduce<E, Long, E, Long, Map<E, Long>> getMapReduce() {
        return GroupCountMapReduce.instance();
    }

    @Override
    public GroupCountStep<S, E> clone() {
        final GroupCountStep<S, E> clone = (GroupCountStep<S, E>) super.clone();
        if (null != this.groupTraversal)
            clone.groupTraversal = clone.integrateChild(this.groupTraversal.clone());
        clone.setBiFunction(new GroupCountBiFunction<>(clone));
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final Traversal.Admin<S, E> traversal : this.getLocalChildren()) {
            result ^= traversal.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.groupTraversal);
    }

    ///////////

    private static class GroupCountBiFunction<S, E> implements BiFunction<Map<E, Long>, Traverser<S>, Map<E, Long>>, Serializable {

        private final GroupCountStep<S, E> groupCountStep;

        private GroupCountBiFunction(final GroupCountStep<S, E> groupCountStep) {
            this.groupCountStep = groupCountStep;

        }

        @Override
        public Map<E, Long> apply(final Map<E, Long> mutatingSeed, final Traverser<S> traverser) {
            MapHelper.incr(mutatingSeed, TraversalUtil.applyNullable(traverser.asAdmin(), this.groupCountStep.groupTraversal), traverser.bulk());
            return mutatingSeed;
        }
    }

    ///////////

    public static final class GroupCountMapReduce<E> extends StaticMapReduce<E, Long, E, Long, Map<E, Long>> {

        private static final GroupCountMapReduce INSTANCE = new GroupCountMapReduce();

        private GroupCountMapReduce() {

        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<E, Long> emitter) {
            final Map<E, Long> groupCount = new HashMap<>();
            vertex.<TraverserSet<Map<E, Long>>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet ->
                    traverserSet.forEach(traverser ->
                            traverser.get().forEach((k, v) -> MapHelper.incr(groupCount, k, (v * traverser.bulk())))));
            groupCount.forEach(emitter::emit);
        }

        @Override
        public void reduce(final E key, final Iterator<Long> values, final ReduceEmitter<E, Long> emitter) {
            long counter = 0;
            while (values.hasNext()) {
                counter = counter + values.next();
            }
            emitter.emit(key, counter);
        }

        @Override
        public void combine(final E key, final Iterator<Long> values, final ReduceEmitter<E, Long> emitter) {
            reduce(key, values, emitter);
        }

        @Override
        public Map<E, Long> generateFinalResult(final Iterator<KeyValue<E, Long>> keyValues) {
            final Map<E, Long> map = new HashMap<>();
            keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
            return map;
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        public static final <E> GroupCountMapReduce<E> instance() {
            return INSTANCE;
        }
    }
}