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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class GraphStepPlaceholder<S, E extends Element> extends AbstractStep<S, E> implements GValueHolder<S, E>, GraphStepContract<S, E> {

    protected final Class<E> returnClass;
    protected GValue<?>[] ids;
    private final boolean isStart;
    private boolean onGraphComputer = false;

    public GraphStepPlaceholder(final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final GValue<?>... ids) {
        super(traversal);
        this.returnClass = returnClass;

        this.ids = ids;
        this.isStart = isStart;

        for (GValue id : ids) {
            traversal.getGValueManager().register(id);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids));
    }

    @Override
    public void close() {} // Nothing to close

    @Override
    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    @Override
    public boolean isStartStep() {
        return this.isStart;
    }

    @Override
    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    @Override
    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    @Override
    public Object[] getIds() {
        for (final GValue<?> id : this.ids) {
            if (id.isVariable()) {
                traversal.getGValueManager().pinVariable(id.getName());
            }
        }
        return GValue.resolveToValues(this.ids);
    }

    @Override
    public GValue<?>[] getIdsAsGValues() {
        return this.ids.clone();
    }

    @Override
    public void clearIds() {
        this.ids = new GValue<?>[0];
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        throw new IllegalStateException("GraphStepGValueContract is not executable");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GraphStepPlaceholder<?, ?> that = (GraphStepPlaceholder<?, ?>) o;
        return isStart == that.isStart &&
                onGraphComputer == that.onGraphComputer &&
                Objects.equals(returnClass, that.returnClass) &&
                Objects.deepEquals(ids, that.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), returnClass, Arrays.hashCode(ids), isStart, onGraphComputer);
    }

    @Override
    public GraphStep<S, E> asConcreteStep() {
        GraphStep<S, E> step = new GraphStep<>(traversal, returnClass, isStart, GValue.resolveToValues(ids));

        if (onGraphComputer) {
            step.onGraphComputer();
        }

        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        for (GValue<?> id : ids) {
            if (id.isVariable()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void updateVariable(String name, Object value) {
        for (int i = 0; i < ids.length; i++) {
            if (name.equals(ids[i].getName())) {
                ids[i] = GValue.of(name, value);
            }
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = new HashSet<>();
        for (GValue<?> id : ids) {
            if (id.isVariable()) {
                gValues.add(id);
            }
        }
        return gValues;
    }

    @Override
    public GraphStepPlaceholder<S, E> clone() {
        GraphStepPlaceholder<S, E> clone = (GraphStepPlaceholder<S, E>) super.clone();
        clone.onGraphComputer = this.onGraphComputer;
        clone.ids = new GValue<?>[this.ids.length];
        for (int i = 0; i < this.ids.length; i++) {
            clone.ids[i] = this.ids[i].clone();
        }
        return clone;
    }
}
