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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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
public class HasContainer implements Serializable, Cloneable, Predicate<Element> {

    private String key;
    private P predicate;

    private final boolean testingIdString;

    public HasContainer(final String key, final P<?> predicate) {
        this.key = key;
        this.predicate = predicate;

        if (!this.key.equals(T.id.getAccessor()))
            testingIdString = false;
        else {
            // the values should be homogenous if a collection is submitted
            final Object predicateValue = this.predicate.getValue();

            // enforce a homogenous collection of values when testing ids
            enforceHomogenousCollectionIfPresent(predicateValue);

            // grab an instance of a value which is either the first item in a homogeneous collection or the value itself
            final Object valueInstance = this.predicate.getValue() instanceof Collection ?
                    ((Collection) this.predicate.getValue()).isEmpty() ?
                            new Object() :
                            ((Collection) this.predicate.getValue()).toArray()[0] :
                    this.predicate.getValue();

            // if the key being evaluated is id then the has() test can evaluate as a toString() representation of the
            // identifier.  this could be done in the test() method but it seems cheaper to do the conversion once in
            // the constructor.  the original value in P is maintained separately
            this.testingIdString = this.key.equals(T.id.getAccessor()) && valueInstance instanceof String;
            if (this.testingIdString)
                this.predicate.setValue(this.predicate.getValue() instanceof Collection ?
                        IteratorUtils.set(IteratorUtils.map(((Collection<Object>) this.predicate.getValue()).iterator(), Object::toString)) :
                        this.predicate.getValue().toString());
        }
    }

    public final boolean test(final Element element) {
        // it is OK to evaluate equality of ids via toString(), given that the test suite enforces the value of
        // id().toString() to be a first class representation of the identifier. a string test is only executed
        // if the predicate value is a String.  this allows stuff like: g.V().has(id,lt(10)) to work properly
        if (this.key.equals(T.id.getAccessor()))
            return testingIdString ? testIdAsString(element) : testId(element);
        if (this.key.equals(T.label.getAccessor()))
            return testLabel(element);

        final Iterator<? extends Property> itty = element.properties(this.key);
        while (itty.hasNext()) {
            if (testValue(itty.next()))
                return true;
        }
        return false;
    }

    public final boolean test(final Property property) {
        if (this.key.equals(T.value.getAccessor()))
            return testValue(property);
        if (this.key.equals(T.key.getAccessor()))
            return testKey(property);
        if (this.key.equals(T.id.getAccessor()) || this.key.equals(T.label.getAccessor())) {
            if (property instanceof Element)
                return test((Element) property);
        }
        return false;
    }

    protected boolean testId(Element element) {
        return this.predicate.test(element.id());
    }

    protected boolean testIdAsString(Element element) {
        return this.predicate.test(element.id().toString());
    }

    protected boolean testLabel(Element element) {
        return this.predicate.test(element.label());
    }

    protected boolean testValue(Property property) {
        return this.predicate.test(property.value());
    }

    protected boolean testKey(Property property) {
        return this.predicate.test(property.key());
    }

    public final String toString() {
        return this.key + '.' + this.predicate;
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

    @Override
    public int hashCode() {
        return (this.key != null ? this.key.hashCode() : 0) ^ (this.predicate != null ? this.predicate.hashCode() : 0);
    }

    public final String getKey() {
        return this.key;
    }

    public final void setKey(final String key) {
        this.key = key;
    }

    public final P<?> getPredicate() {
        return this.predicate;
    }

    public final BiPredicate<?, ?> getBiPredicate() {
        return this.predicate.getBiPredicate();
    }

    public final Object getValue() {
        return this.predicate.getValue();
    }

    ////////////

    private void enforceHomogenousCollectionIfPresent(final Object predicateValue) {
        if (predicateValue instanceof Collection) {
            final Collection collection = (Collection) predicateValue;
            if (!collection.isEmpty()) {
                Class<?> first = collection.toArray()[0].getClass();
                if (!((Collection) predicateValue).stream().map(Object::getClass).allMatch(c -> first.equals(c)))
                    throw new IllegalArgumentException("Has comparisons on a collection of ids require ids to all be of the same type");
            }
        }
    }

    public static <S> boolean testAll(final S element, final List<HasContainer> hasContainers) {
        boolean isProperty = element instanceof Property;
        for (final HasContainer hasContainer : hasContainers) {
            if (isProperty) {
                if (!hasContainer.test((Property) element))
                    return false;
            } else {
                if (!hasContainer.test((Element) element))
                    return false;
            }
        }
        return true;
    }
}