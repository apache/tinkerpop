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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasContainer implements Serializable {

    public String key;
    public BiPredicate predicate;
    public Object value;

    public HasContainer(final String key, final BiPredicate predicate, final Object value) {
        this.key = key;
        this.predicate = predicate;
        this.value = value;
        if (null == this.value && !(this.predicate instanceof Contains)) {
            throw new IllegalArgumentException("For determining the existence of a property, use the Contains predicate");
        }
    }

    public HasContainer(final String key, final Contains contains) {
        this(key, contains, null);
    }

    public HasContainer(final T accessor, final BiPredicate predicate, final Object value) {
        this(accessor.getAccessor(), predicate, value);
    }

    public HasContainer(final T accessor, final Contains contains) {
        this(accessor.getAccessor(), contains, null);
    }

    public boolean test(final Element element) {
        if (null != this.value) {
            // it is OK to evaluate equality of ids via toString() now given that the toString() the test suite
            // enforces the value of id.()toString() to be a first class representation of the identifier
            if (this.key.equals(T.id.getAccessor()))
                return this.predicate.test(element.id().toString(), this.value.toString());
            else if (this.key.equals(T.label.getAccessor()))
                return this.predicate.test(element.label(), this.value);
            else if (element instanceof VertexProperty && this.key.equals(T.value.getAccessor()))
                return this.predicate.test(((VertexProperty) element).value(), this.value);
            else if (element instanceof VertexProperty && this.key.equals(T.key.getAccessor()))
                return this.predicate.test(((VertexProperty) element).key(), this.value);
            else {
                if (element instanceof Vertex) {
                    final Iterator<? extends Property> itty = element.properties(this.key);
                    while (itty.hasNext()) {
                        if (this.predicate.test(itty.next().value(), this.value))
                            return true;
                    }
                    return false;
                } else {
                    final Property property = element.property(this.key);
                    return property.isPresent() && this.predicate.test(property.value(), this.value);
                }
            }
        } else {
            return Contains.within.equals(this.predicate) ?
                    element.property(this.key).isPresent() :
                    !element.property(this.key).isPresent();
        }
    }

    public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
        if (hasContainers.size() == 0)
            return true;
        else {
            for (final HasContainer hasContainer : hasContainers) {
                if (!hasContainer.test(element))
                    return false;
            }
            return true;
        }
    }

    // note that if the user is looking for a label property key (e.g.), then it will look the same as looking for the label of the element.
    public String toString() {
        return this.value == null ?
                (this.predicate == Contains.within ?
                        '[' + this.key + ']' :
                        "[!" + this.key + ']') :
                '[' + this.key + ',' + this.predicate + ',' + this.value + ']';
    }
}