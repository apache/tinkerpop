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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S, E> extends ReducingBarrierStep<S, Map<E, Long>> implements TraversalParent, ByModulating {

    private Traversal.Admin<S, E> keyTraversal = null;

    public GroupCountStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(HashMapSupplier.instance());
        this.setReducingBiOperator(GroupCountBiOperator.instance());
    }

    @Override
    public Map<E, Long> projectTraverser(final Traverser.Admin<S> traverser) {
        final Map<E, Long> map = new HashMap<>(1);
        map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), traverser.bulk());
        return map;
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> groupTraversal) {
        this.keyTraversal = this.integrateChild(groupTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.keyTraversal ? Collections.emptyList() : Collections.singletonList(this.keyTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> keyTraversal) throws UnsupportedOperationException {
        this.keyTraversal = this.integrateChild(keyTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.keyTraversal && this.keyTraversal.equals(oldTraversal))
            this.keyTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public GroupCountStep<S, E> clone() {
        final GroupCountStep<S, E> clone = (GroupCountStep<S, E>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = this.keyTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.keyTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal);
    }

    ///////////////////////////

    public static final class GroupCountBiOperator<E> implements BinaryOperator<Map<E, Long>>, Serializable {

        private static final GroupCountBiOperator INSTANCE = new GroupCountBiOperator();

        @Override
        public Map<E, Long> apply(final Map<E, Long> mutatingSeed, final Map<E, Long> map) {
            for (final Map.Entry<E, Long> entry : map.entrySet()) {
                MapHelper.incr(mutatingSeed, entry.getKey(), entry.getValue());
            }
            return mutatingSeed;
        }

        public static final <E> GroupCountBiOperator<E> instance() {
            return INSTANCE;
        }
    }
}
