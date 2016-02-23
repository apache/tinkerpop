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
        this.setReducingBiOperator(new GroupCountBiOperator());
    }


    @Override
    public void modulateBy(final Traversal.Admin<?, ?> keyTraversal) {
        this.keyTraversal = this.integrateChild(keyTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.keyTraversal ? Collections.emptyList() : Collections.singletonList(this.keyTraversal);
    }

    public Map<E, Long> projectTraverser(final Traverser.Admin<S> traverser) {
        return Collections.singletonMap(TraversalUtil.applyNullable(traverser, this.keyTraversal), traverser.bulk());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    @Override
    public GroupCountStep<S, E> clone() {
        final GroupCountStep<S, E> clone = (GroupCountStep<S, E>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
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
        return StringFactory.stepString(this, this.keyTraversal);
    }

    ///////////

    public static final class GroupCountBiOperator<E> implements BinaryOperator<Map<E, Long>>, Serializable {

        @Override
        public Map<E, Long> apply(final Map<E, Long> mutatingSeed, final Map<E, Long> map) {
            map.forEach((k, v) -> MapHelper.incr(mutatingSeed, k, v));
            return mutatingSeed;
        }
    }
}