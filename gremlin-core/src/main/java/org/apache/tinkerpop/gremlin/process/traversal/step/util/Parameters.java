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

import org.apache.commons.lang.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The parameters held by a {@link Traversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Parameters implements Cloneable, Serializable {

    private static final Object[] EMPTY_ARRAY = new Object[0];

    private Map<Object, List<Object>> parameters = new HashMap<>();
    private Set<String> referencedLabels = new HashSet<>();

    /**
     * A cached list of traversals that serve as parameter values. The list is cached on calls to
     * {@link #set(Object...)} because when the parameter map is large the cost of iterating it repeatedly on the
     * high number of calls to {@link #getTraversals()} and {@link #integrateTraversals(TraversalParent)} is great.
     */
    private List<Traversal.Admin<?,?>> traversals = new ArrayList<>();

    /**
     * Checks for existence of key in parameter set.
     *
     * @param key the key to check
     * @return {@code true} if the key is present and {@code false} otherwise
     */
    public boolean contains(final Object key) {
        return this.parameters.containsKey(key);
    }

    /**
     * Renames a key in the parameter set.
     *
     * @param oldKey the key to rename
     * @param newKey the new name of the key
     */
    public void rename(final Object oldKey, final Object newKey) {
        this.parameters.put(newKey, this.parameters.remove(oldKey));
    }

    /**
     * Gets the list of values for a key, while resolving the values of any parameters that are {@link Traversal}
     * objects.
     */
    public <S, E> List<E> get(final Traverser.Admin<S> traverser, final Object key, final Supplier<E> defaultValue) {
        final List<E> values = (List<E>) this.parameters.get(key);
        if (null == values) return Collections.singletonList(defaultValue.get());
        final List<E> result = new ArrayList<>();
        for (final Object value : values) {
            result.add(value instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, E>) value) : (E) value);
        }
        return result;
    }

    /**
     * Gets the value of a key and if that key isn't present returns the default value from the {@link Supplier}.
     *
     * @param key          the key to retrieve
     * @param defaultValue the default value generator
     */
    public <E> List<E> get(final Object key, final Supplier<E> defaultValue) {
        final List<E> list = (List<E>) this.parameters.get(key);
        return (null == list) ? Collections.singletonList(defaultValue.get()) : list;

    }

    /**
     * Remove a key from the parameter set.
     *
     * @param key the key to remove
     * @return the value of the removed key
     */
    public Object remove(final Object key) {
        final List<Object> o = parameters.remove(key);

        // once a key is removed, it's possible that the traversal/label cache will need to be regenerated
        if (IteratorUtils.anyMatch(o.iterator(), p -> p instanceof Traversal.Admin)) {
            traversals.clear();
            traversals = new ArrayList<>();
            for (final List<Object> list : this.parameters.values()) {
                for (final Object object : list) {
                    if (object instanceof Traversal.Admin) {
                        final Traversal.Admin t = (Traversal.Admin) object;
                        addTraversal(t);
                    }
                }
            }
        }

        return o;
    }

    /**
     * Gets the array of keys/values of the parameters while resolving parameter values that contain
     * {@link Traversal} instances.
     */
    public <S> Object[] getKeyValues(final Traverser.Admin<S> traverser, final Object... exceptKeys) {
        if (this.parameters.isEmpty()) return EMPTY_ARRAY;
        final List<Object> keyValues = new ArrayList<>();
        for (final Map.Entry<Object, List<Object>> entry : this.parameters.entrySet()) {
            if (!ArrayUtils.contains(exceptKeys, entry.getKey())) {
                for (final Object value : entry.getValue()) {
                    keyValues.add(entry.getKey() instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) entry.getKey()) : entry.getKey());
                    keyValues.add(value instanceof Traversal.Admin ? TraversalUtil.apply(traverser, (Traversal.Admin<S, ?>) value) : value);
                }
            }
        }
        return keyValues.toArray(new Object[keyValues.size()]);
    }

    /**
     * Gets an immutable set of the parameters without evaluating them in the context of a {@link Traverser} as
     * is done in {@link #getKeyValues(Traverser.Admin, Object...)}.
     *
     * @param exceptKeys keys to not include in the returned {@link Map}
     */
    public Map<Object, List<Object>> getRaw(final Object... exceptKeys) {
        if (parameters.isEmpty()) return Collections.emptyMap();
        final List<Object> exceptions = Arrays.asList(exceptKeys);
        final Map<Object, List<Object>> raw = new HashMap<>();
        for (Map.Entry<Object, List<Object>> entry : parameters.entrySet()) {
            if (!exceptions.contains(entry.getKey())) raw.put(entry.getKey(), entry.getValue());
        }

        return Collections.unmodifiableMap(raw);
    }

    /**
     * Set parameters given key/value pairs.
     */
    public void set(final Object... keyValues) {
        Parameters.legalPropertyKeyValueArray(keyValues);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i + 1] != null) {
                // track the list of traversals that are present so that elsewhere in Parameters there is no need
                // to iterate all values to not find any. also grab available labels in traversal values
                if (keyValues[i + 1] instanceof Traversal.Admin) {
                    final Traversal.Admin t = (Traversal.Admin) keyValues[i + 1];
                    addTraversal(t);
                }

                List<Object> values = this.parameters.get(keyValues[i]);
                if (null == values) {
                    values = new ArrayList<>();
                    values.add(keyValues[i + 1]);
                    this.parameters.put(keyValues[i], values);
                } else {
                    values.add(keyValues[i + 1]);
                }
            }
        }
    }

    /**
     * Calls {@link TraversalParent#integrateChild(Traversal.Admin)} on any traversal objects in the parameter list.
     * This method requires that {@link #set(Object...)} is called prior to this method as the it will switch the
     * {@code traversalsPresent} flag field if a {@link Traversal.Admin} object is present and allow this method to
     * do its work.
     */
    public void integrateTraversals(final TraversalParent step) {
        for (Traversal.Admin t : traversals) {
            step.integrateChild(t);
        }
    }

    /**
     * Gets all the {@link Traversal.Admin} objects in the map of parameters.
     */
    public <S, E> List<Traversal.Admin<S, E>> getTraversals() {
        // stupid generics - just need to return "traversals"
        return (List<Traversal.Admin<S, E>>) (Object) traversals;
    }

    /**
     * Gets a list of all labels held in parameters that have a traversal as a value.
     */
    public Set<String> getReferencedLabels() {
        return referencedLabels;
    }

    public Parameters clone() {
        try {
            final Parameters clone = (Parameters) super.clone();
            clone.parameters = new HashMap<>();
            for (final Map.Entry<Object, List<Object>> entry : this.parameters.entrySet()) {
                final List<Object> values = new ArrayList<>();
                for (final Object value : entry.getValue()) {
                    values.add(value instanceof Traversal.Admin ? ((Traversal.Admin) value).clone() : value);
                }
                clone.parameters.put(entry.getKey() instanceof Traversal.Admin ? ((Traversal.Admin) entry.getKey()).clone() : entry.getKey(), values);
            }

            clone.referencedLabels = new HashSet<>(this.referencedLabels);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public int hashCode() {
        int result = 1;
        for (final Map.Entry<Object, List<Object>> entry : this.parameters.entrySet()) {
            result ^= entry.getKey().hashCode();
            for (final Object value : entry.getValue()) {
                result ^= Integer.rotateLeft(value.hashCode(), entry.getKey().hashCode());
            }
        }
        return result;
    }

    public String toString() {
        return this.parameters.toString();
    }

    private static void legalPropertyKeyValueArray(final Object... propertyKeyValues) throws IllegalArgumentException {
        if (propertyKeyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!(propertyKeyValues[i] instanceof String) && !(propertyKeyValues[i] instanceof T) && !(propertyKeyValues[i] instanceof Traversal))
                throw new IllegalArgumentException("The provided key/value array must have a String, T, or Traversal on even array indices");
        }
    }

    private void addTraversal(final Traversal.Admin t) {
        traversals.add(t);
        for (final Object ss : t.getSteps()) {
            if (ss instanceof Scoping) {
                for (String label : ((Scoping) ss).getScopeKeys()) {
                    referencedLabels.add(label);
                }
            }
        }
    }
}
