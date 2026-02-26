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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Converts a {@link Element} to a {@code Map}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElementMapStep<K,E> extends ScalarMapStep<Element, Map<K, E>> implements TraversalParent, GraphComputing, Configuring {

    protected final String[] propertyKeys;
    private boolean onGraphComputer = false;
    private boolean multilabel = false;
    private final Parameters parameters = new Parameters();

    public ElementMapStep(final Traversal.Admin traversal, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0].equals(WithOptions.multilabel)) {
            this.multilabel = true;
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new LinkedHashMap<>();
        final Element element = traverser.get();
        map.put(T.id, element.id());
        if (element instanceof VertexProperty) {
            map.put(T.key, ((VertexProperty<?>) element).key());
            map.put(T.value, ((VertexProperty<?>) element).value());
        } else {
            if (isMultilabelEnabled()) {
                map.put(T.label, element.labels());
            } else {
                map.put(T.label, element.label());
            }
        }

        if (element instanceof Edge) {
            final Edge e = (Edge) element;
            map.put(Direction.IN, getVertexStructure(e.inVertex()));
            map.put(Direction.OUT, getVertexStructure(e.outVertex()));
        }

        final Iterator<? extends Property> properties = element.properties(this.propertyKeys);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            map.put(property.key(), property.value());
        }

        return (Map) map;
    }

    protected Map<Object, Object> getVertexStructure(final Vertex v) {
        final Map<Object, Object> m = new LinkedHashMap<>();
        m.put(T.id, v.id());

        // can't add label if doing GraphComputer stuff as there is no access to the label of the adjacent vertex
        if (!onGraphComputer) m.put(T.label, v.label());

        return m;
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    public boolean isOnGraphComputer() {
        return onGraphComputer;
    }

    /**
     * Checks if multilabel mode is enabled either via step-level {@code .with(WithOptions.multilabel)}
     * or source-level {@code g.with("multilabel")}.
     */
    private boolean isMultilabelEnabled() {
        if (this.multilabel) return true;
        return getTraversal().getStrategies().getStrategy(OptionsStrategy.class)
                .map(os -> os.getOptions().containsKey("multilabel") || os.getOptions().containsKey(WithOptions.multilabel))
                .orElse(false);
    }

    public String[] getPropertyKeys() {
        return propertyKeys;
    }

    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys));
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final String propertyKey : this.propertyKeys) {
            result ^= Objects.hashCode(propertyKey);
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
