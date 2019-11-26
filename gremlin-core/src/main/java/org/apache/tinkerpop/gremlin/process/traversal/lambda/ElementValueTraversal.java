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

import java.util.Map;
import java.util.Objects;

/**
 * More efficiently extracts a value from an {@link Element} or {@code Map} to avoid strategy application costs. Note
 * that as of 3.4.5 this class is now poorly named as it was originally designed to work with {@link Element} only.
 * In future 3.5.0 this class will likely be renamed but to ensure that we get this revised functionality for
 * {@code Map} without introducing a breaking change this name will remain the same.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ElementValueTraversal<V> extends AbstractLambdaTraversal<Element, V> {

    private final String propertyKey;
    private V value;

    public ElementValueTraversal(final String propertyKey) {
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
    public void addStart(final Traverser.Admin<Element> start) {
        if (null == this.bypassTraversal) {
            // playing a game of type erasure here.....technically we can process either Map or Element here in this
            // case after 3.4.5. and obviously users get weird errors along those lines here anyway. will fix up the
            // generics in 3.5.0 when we can take some breaking changes. seemed like this feature would make Gremlin
            // a lot nicer and given the small footprint of the change this seemed like a good hack to take.
            final Object o = start.get();
            if (o instanceof Element)
                this.value = ((Element) o).value(propertyKey);
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
        return other instanceof ElementValueTraversal
                && Objects.equals(((ElementValueTraversal) other).propertyKey, this.propertyKey);
    }
}