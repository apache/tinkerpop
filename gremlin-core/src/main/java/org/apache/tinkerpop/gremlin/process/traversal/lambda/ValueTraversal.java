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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;

import java.util.Map;
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

    public ValueTraversal(final String propertyKey) {
        this.propertyKey = propertyKey;
    }

    @Override
    public V next() {
        return this.value;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public void addStart(final Traverser.Admin<T> start) {
        if (null == this.bypassTraversal) {
            final T o = start.get();
            if (o instanceof Element)
                this.value = (V)((Element) o).property(propertyKey).orElse(null);
            else if (o instanceof Map)
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
    public String toString() {
        return "value(" + (null == this.bypassTraversal ? this.propertyKey : this.bypassTraversal) + ')';
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.propertyKey.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueTraversal
                && Objects.equals(((ValueTraversal) other).propertyKey, this.propertyKey);
    }
}