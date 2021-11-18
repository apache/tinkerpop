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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * More efficiently extracts a value from an {@link Element} or {@code Map} to avoid strategy application costs.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ValueTraversal<T, V> extends AbstractLambdaTraversal<T, V> {

    private final String propertyKey;
    private V value;
    private boolean noStarts = false;

    /**
     * Creates an instance for the specified {@code propertyKey}.
     */
    public ValueTraversal(final String propertyKey) {
        this.propertyKey = propertyKey;
    }

    /**
     * Creates an instance with the {@code bypassTraversal} set on construction.
     */
    public ValueTraversal(final String propertyKey, final Traversal.Admin<T,V> bypassTraversal) {
        this.propertyKey = propertyKey;
        this.setBypassTraversal(bypassTraversal);
    }

    /**
     * Gets the value of the {@link Traversal} which will be {@code null} until {@link #addStart(Traverser.Admin)} is
     * called.
     */
    public V getValue() {
        return value;
    }

    /**
     * Determines if there is a value to iterate from the {@link Traversal}.
     */
    public boolean isNoStarts() {
        return noStarts;
    }

    /**
     * Return the {@code value} of the traverser and will continue to return it on subsequent calls. Calling methods
     * should account for this behavior on their own.
     */
    @Override
    public V next() {
        if (noStarts) throw new NoSuchElementException(String.format("%s is empty", this.toString()));
        return this.value;
    }

    /**
     * If there is a "start" traverser this method will return {@code true} and otherwise {@code false} and in either
     * case will always return as such, irrespective of calls to {@link #next()}. Calling methods should account for
     * this behavior on their own.
     */
    @Override
    public boolean hasNext() {
        // value traversal is a bit of a special case for next/hasNext(). prior to "noStarts" this method would always
        // return true and next() would always return the value. in other words if the ValueTraversal had a successful
        // addStart(Traverser) it would indefinitely return the start. other parts of the processing engine must
        // account for this behavior and only call the ValueTraversal once. With the change to use "noStarts" we rely
        // on that same behavior in the processing engine and assume a single call but now hasNext() will always
        // either always be true or always be false and next() will throw NoSuchElementException
        return !noStarts;
    }

    @Override
    public void addStart(final Traverser.Admin<T> start) {
        if (null == this.bypassTraversal) {
            final T o = start.get();
            if (o instanceof Element) {
                // if the property is not present then it has noStarts which means hasNext() return false
                final Property<V> p = ((Element) o).property(propertyKey);
                if (p.isPresent())
                    this.value = p.value();
                else
                    noStarts = true;
            } else if (o instanceof Map)
                this.value = (V) ((Map) o).get(propertyKey);
            else
                throw new IllegalStateException(String.format(
                        "The by(\"%s\") modulator can only be applied to a traverser that is an Element or a Map - it is being applied to [%s] a %s class instead",
                        propertyKey, o, o.getClass().getSimpleName()));
        } else {
            this.value = TraversalUtil.apply(start, this.bypassTraversal);
        }
    }

    public String getPropertyKey() {
        return this.propertyKey;
    }

    @Override
    public void reset() {
        super.reset();
        this.noStarts = false;
    }

    @Override
    public String toString() {
        return "value(" + (null == this.bypassTraversal ? this.propertyKey : this.bypassTraversal) + ')';
    }

    @Override
    public int hashCode() {
        int hc = 19;
        hc = 43 * hc + super.hashCode();
        hc = 43 * hc + this.propertyKey.hashCode();
        return hc;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueTraversal
                && Objects.equals(((ValueTraversal) other).propertyKey, this.propertyKey);
    }
}