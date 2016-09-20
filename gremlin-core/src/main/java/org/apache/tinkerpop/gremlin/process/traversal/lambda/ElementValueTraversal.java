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

/**
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
        this.value = null == this.bypassTraversal ? start.get().value(this.propertyKey) : TraversalUtil.apply(start, this.bypassTraversal);
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
}
