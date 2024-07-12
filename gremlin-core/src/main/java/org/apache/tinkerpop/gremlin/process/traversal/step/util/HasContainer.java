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
import org.apache.tinkerpop.gremlin.process.traversal.step.GType;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
        this.testingIdString = isStringTestable();
    }

    public final boolean test(final Element element) {
        // it is OK to evaluate equality of ids via toString(), given that the test suite enforces the value of
        // id().toString() to be a first class representation of the identifier. a string test is only executed
        // if the predicate value is a String.  this allows stuff like: g.V().has(id,lt(10)) to work properly
        if (this.key != null) {
            if (this.key.equals(T.id.getAccessor()))
                return testingIdString ? testIdAsString(element) : testId(element);
            if (this.key.equals(T.label.getAccessor()))
                return testLabel(element);
        }

        final Iterator<? extends Property> itty = element.properties(this.key);
        try {
            while (itty.hasNext()) {
                if (testValue(itty.next()))
                    return true;
            }
        } finally {
            CloseableIterator.closeIterator(itty);
        }
        return false;
    }

    public final boolean test(final Property property) {
        if (this.key != null) {
            if (this.key.equals(T.value.getAccessor()))
                return testValue(property);
            if (this.key.equals(T.key.getAccessor()))
                return testKey(property);
        }
        if (property instanceof Element)
            return test((Element) property);
        return false;
    }

    protected boolean testId(final Element element) {
        return this.predicate.test(element.id());
    }

    protected boolean testIdAsString(final Element element) {
        return this.predicate.test(element.id().toString());
    }

    protected boolean testLabel(final Element element) {
        return this.predicate.test(element.label());
    }

    protected boolean testValue(final Property property) {
        return this.predicate.test(property.value());
    }

    protected boolean testKey(final Property property) {
        return this.predicate.test(property.key());
    }

    public final String toString() {
        return Objects.toString(this.key) + '.' + this.predicate;
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

    /**
     * Determines if the value of the predicate is testable via {@code toString()} representation of an element which
     * is only relevant if the has relates to an {@link T#id}.
     */
    private boolean isStringTestable() {
        if (this.key != null && this.key.equals(T.id.getAccessor())) {
            final Object predicateValue = null == this.predicate ? null : this.predicate.getValue();
            if (predicateValue instanceof Collection) {
                final Collection collection = (Collection) predicateValue;
                if (!collection.isEmpty()) {
                    return ((Collection) predicateValue).stream().allMatch(c -> null == c || GValue.instanceOf(c, GType.STRING));
                }
            }

            return GValue.instanceOf(predicateValue, GType.STRING);
        }

        return false;
    }

    public static <V> boolean testAll(final Property<V> property, final List<HasContainer> hasContainers) {
        return internalTestAll(property, hasContainers);
    }

    public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
        return internalTestAll(element, hasContainers);
    }

    private static <S> boolean internalTestAll(final S element, final List<HasContainer> hasContainers) {
        final boolean isProperty = element instanceof Property;
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