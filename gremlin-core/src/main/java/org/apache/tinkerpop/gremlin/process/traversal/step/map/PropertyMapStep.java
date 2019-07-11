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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class PropertyMapStep<K,E> extends MapStep<Element, Map<K, E>>
        implements TraversalParent, ByModulating, Configuring {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;

    protected int tokens;
    protected Traversal.Admin<Element, ? extends Property> propertyTraversal;

    private Parameters parameters = new Parameters();
    private TraversalRing<K, E> traversalRing;

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #PropertyMapStep(Traversal.Admin, PropertyType, String...)}.
     */
    @Deprecated
    public PropertyMapStep(final Traversal.Admin traversal, final boolean includeTokens, final PropertyType propertyType, final String... propertyKeys) {
        this(traversal, propertyType, propertyKeys);
        this.configure(WithOptions.tokens, includeTokens ? WithOptions.all : WithOptions.none);
    }

    public PropertyMapStep(final Traversal.Admin traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.returnType = propertyType;
        this.propertyTraversal = null;
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new LinkedHashMap<>();
        final Element element = traverser.get();
        final boolean isVertex = element instanceof Vertex;
        if (this.returnType == PropertyType.VALUE) {
            if (includeToken(WithOptions.ids)) map.put(T.id, element.id());
            if (element instanceof VertexProperty) {
                if (includeToken(WithOptions.keys)) map.put(T.key, ((VertexProperty<?>) element).key());
                if (includeToken(WithOptions.values)) map.put(T.value, ((VertexProperty<?>) element).value());
            } else {
                if (includeToken(WithOptions.labels)) map.put(T.label, element.label());
            }
        }
        final Iterator<? extends Property> properties = null == this.propertyTraversal ?
                element.properties(this.propertyKeys) :
                TraversalUtil.applyAll(traverser, this.propertyTraversal);
        //final Iterator<? extends Property> properties = element.properties(this.propertyKeys);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            final Object value = this.returnType == PropertyType.VALUE ? property.value() : property;
            if (isVertex) {
                map.compute(property.key(), (k, v) -> {
                    final List<Object> values = v != null ? (List<Object>) v : new ArrayList<>();
                    values.add(value);
                    return values;
                });
            } else {
                map.put(property.key(), value);
            }
        }
        if (!traversalRing.isEmpty()) {
            for (final Object key : map.keySet()) {
                map.compute(key, (k, v) -> TraversalUtil.applyNullable(v, (Traversal.Admin) this.traversalRing.next()));
            }
            this.traversalRing.reset();
        }
        return (Map) map;
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0].equals(WithOptions.tokens)) {
            if (keyValues.length == 2 && keyValues[1] instanceof Boolean) {
                this.tokens = ((boolean) keyValues[1]) ? WithOptions.all : WithOptions.none;
            } else {
                for (int i = 1; i < keyValues.length; i++) {
                    if (!(keyValues[i] instanceof Integer))
                        throw new IllegalArgumentException("WithOptions.tokens requires Integer arguments (possible " + "" +
                                "values are: WithOptions.[none|ids|labels|keys|values|all])");
                    this.tokens |= (int) keyValues[i];
                }
            }
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public List<Traversal.Admin<K, E>> getLocalChildren() {
        final List<Traversal.Admin<K, E>> result = new ArrayList<>();
        if (null != this.propertyTraversal)
            result.add((Traversal.Admin) propertyTraversal);
        result.addAll(this.traversalRing.getTraversals());
        return Collections.unmodifiableList(result);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    public void setPropertyTraversal(final Traversal.Admin<Element, ? extends Property> propertyTraversal) {
        this.propertyTraversal = this.integrateChild(propertyTraversal);
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #getIncludedTokens()}.
     */
    @Deprecated
    public boolean isIncludeTokens() {
        return this.tokens != WithOptions.none;
    }

    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys),
                this.traversalRing, this.returnType.name().toLowerCase());
    }

    @Override
    public PropertyMapStep<K,E> clone() {
        final PropertyMapStep<K,E> clone = (PropertyMapStep<K,E>) super.clone();
        if (null != this.propertyTraversal)
            clone.propertyTraversal = this.propertyTraversal.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnType.hashCode() ^ Integer.hashCode(this.tokens);
        if (null != this.propertyTraversal)
            result ^= this.propertyTraversal.hashCode();
        for (final String propertyKey : this.propertyKeys) {
            result ^= propertyKey.hashCode();
        }
        return result ^ this.traversalRing.hashCode();
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (null != this.propertyTraversal)
            this.integrateChild(this.propertyTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    public int getIncludedTokens() {
        return this.tokens;
    }

    private boolean includeToken(final int token) {
        return 0 != (this.tokens & token);
    }
}
