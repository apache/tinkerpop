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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyMapStep<E> extends MapStep<Element, Map<String, E>> implements TraversalParent {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;
    protected final boolean includeTokens;
    protected Traversal.Admin<Element, ? extends Property> propertyTraversal;

    public PropertyMapStep(final Traversal.Admin traversal, final boolean includeTokens, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.includeTokens = includeTokens;
        this.propertyKeys = propertyKeys;
        this.returnType = propertyType;
        this.propertyTraversal = null;
    }

    @Override
    protected Map<String, E> map(final Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new HashMap<>();
        final Element element = traverser.get();
        final boolean isVertex = traverser.get() instanceof Vertex;
        final Iterator<? extends Property> properties = null == this.propertyTraversal ?
                (Iterator) element.properties(this.propertyKeys) :
                TraversalUtil.applyAll(traverser, this.propertyTraversal);
        while (properties.hasNext()) {
            final Property property = properties.next();
            if (isVertex) {
                List values = (List) map.get(property.key());
                if (null == values) {
                    values = new ArrayList();
                    map.put(property.key(), values);
                }
                values.add(this.returnType == PropertyType.VALUE ? property.value() : property);
            } else
                map.put(property.key(), this.returnType == PropertyType.VALUE ? property.value() : property);
        }
        if (this.returnType == PropertyType.VALUE && this.includeTokens) {
            if (element instanceof VertexProperty) {
                map.put(T.id, element.id());
                map.put(T.key, ((VertexProperty) element).key());
                map.put(T.value, ((VertexProperty) element).value());
            } else {
                map.put(T.id, element.id());
                map.put(T.label, element.label());
            }
        }
        return (Map) map;
    }

    @Override
    public List<Traversal.Admin<Element, ? extends Property>> getLocalChildren() {
        return null == this.propertyTraversal ? Collections.emptyList() : Collections.singletonList(this.propertyTraversal);
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

    public boolean isIncludeTokens() {
        return includeTokens;
    }

    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys), this.returnType.name().toLowerCase());
    }

    @Override
    public PropertyMapStep<E> clone() {
        final PropertyMapStep<E> clone = (PropertyMapStep<E>) super.clone();
        if (null != this.propertyTraversal)
            clone.propertyTraversal = this.propertyTraversal.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnType.hashCode() ^ Boolean.hashCode(this.includeTokens);
        if (null == this.propertyTraversal) {
            for (final String propertyKey : this.propertyKeys) {
                result ^= propertyKey.hashCode();
            }
        } else {
            result ^= this.propertyTraversal.hashCode();
        }
        return result;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (null != this.propertyTraversal)
            this.integrateChild(this.propertyTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
