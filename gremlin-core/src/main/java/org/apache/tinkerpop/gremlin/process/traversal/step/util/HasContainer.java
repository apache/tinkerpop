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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasContainer implements Serializable, Cloneable, Predicate<Element> {

    private String key;
    private P predicate;

    public HasContainer(final String key, final P<?> predicate) {
        this.key = key;
        this.predicate = predicate;

        // if the key being evaluated is id then the has() test can evaluate as a toString() representation of the
        // identifier.  this could be done in the test() method but it seems cheaper to do the conversion once in
        // the constructor.  to avoid losing the original value, the string version of the collection is maintained
        // separately
        if (this.key.equals(T.id.getAccessor()))
            this.predicate.setValue(this.predicate.getValue() instanceof Collection ? IteratorUtils.set(IteratorUtils.map(((Collection<Object>) this.predicate.getValue()).iterator(), Object::toString)) : this.predicate.getValue().toString());
    }

    public boolean test(final Element element) {
        // it is OK to evaluate equality of ids via toString() now given that the toString() the test suite
        // enforces the value of id().toString() to be a first class representation of the identifier
        if (this.key.equals(T.id.getAccessor()))
            return this.predicate.test(element.id().toString());
        else if (this.key.equals(T.label.getAccessor()))
            return this.predicate.test(element.label());
        else if (element instanceof VertexProperty && this.key.equals(T.value.getAccessor()))
            return this.predicate.test(((VertexProperty) element).value());
        else if (element instanceof VertexProperty && this.key.equals(T.key.getAccessor()))
            return this.predicate.test(((VertexProperty) element).key());
        else {
            if (element instanceof Vertex) {
                final Iterator<? extends Property> itty = element.properties(this.key);
                while (itty.hasNext()) {
                    if (this.predicate.test(itty.next().value()))
                        return true;
                }
                return false;
            } else {
                final Property property = element.property(this.key);
                return property.isPresent() && this.predicate.test(property.value());
            }
        }
    }

    public String toString() {
        return '[' + this.key + ',' + this.predicate + ']';
    }

    public HasContainer clone() {
        try {
            final HasContainer clone = (HasContainer) super.clone();
            clone.predicate = this.predicate.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public P<?> getPredicate() {
        return this.predicate;
    }

    public BiPredicate<?, ?> getBiPredicate() {
        return this.predicate.getBiPredicate();
    }

    public Object getValue() {
        return this.predicate.getValue();
    }

    ////////////

    public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
        for (final HasContainer hasContainer : hasContainers) {
            if (!hasContainer.test(element))
                return false;
        }
        return true;
    }

    public static HasContainer[] makeHasContainers(final String key, final P<?> predicate) {
        if (predicate instanceof AndP) {
            final List<P<?>> predicates = ((AndP) predicate).getPredicates();
            final HasContainer[] hasContainers = new HasContainer[predicates.size()];
            for (int i = 0; i < predicates.size(); i++) {
                hasContainers[i] = new HasContainer(key, predicate);
            }
            return hasContainers;
        } else if (predicate instanceof OrP) {
            throw new IllegalArgumentException("The or'ing of HasContainers is currently not supported");
        } else {
            return new HasContainer[]{new HasContainer(key, predicate)};
        }
    }
}