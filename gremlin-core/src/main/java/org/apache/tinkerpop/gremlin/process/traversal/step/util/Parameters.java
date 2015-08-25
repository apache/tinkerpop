/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Parameters implements Cloneable, Serializable {

    private static final Object[] EMPTY_ARRAY = new Object[0];

    private Map<Object, Object> parameters = new HashMap<>();

    public <S, E> E get(final Traverser.Admin<S> traverser, final Object key) {
        final Object object = parameters.get(key);
        return object instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, E>) object) : (E) object;
    }

    public <S> Object[] getKeyValues(final Traverser.Admin<S> traverser, final Object... exceptKeys) {
        if (this.parameters.size() == 0) return EMPTY_ARRAY;
        final List<Object> exceptions = Arrays.asList(exceptKeys);
        final Object[] keyValues = new Object[(this.parameters.size() * 2) - (exceptKeys.length * 2)];
        int counter = 0;
        for (final Map.Entry<Object, Object> keyValue : this.parameters.entrySet()) {
            if (!exceptions.contains(keyValue.getKey())) {
                keyValues[counter++] = keyValue.getKey() instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) keyValue.getKey()) : keyValue.getKey();
                keyValues[counter++] = keyValue.getValue() instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) keyValue.getValue()) : keyValue.getValue();
            }
        }
        return keyValues;
    }

    public void set(final Object key, final Object value) {
        if (null != value) this.parameters.put(key, value);
    }

    public void integrateTraversals(final TraversalParent step) {
        for (final Object value : this.parameters.values()) {
            if (value instanceof Traversal.Admin) {
                step.integrateChild((Traversal.Admin) value);
            }
        }
    }

    public <S, E> List<Traversal.Admin<S, E>> getTraversals() {
        return (List) this.parameters.values().stream().filter(t -> t instanceof Traversal.Admin).collect(Collectors.toList());
    }

    public Parameters clone() {
        try {
            final Parameters clone = (Parameters) super.clone();
            clone.parameters = new HashMap<>();
            for (final Map.Entry<Object, Object> entry : this.parameters.entrySet()) {
                clone.parameters.put(entry.getKey(), entry.getValue() instanceof Traversal ? ((Traversal.Admin) entry.getValue()).clone() : entry.getValue());
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public int hashCode() {
        return this.parameters.hashCode();
    }
}
