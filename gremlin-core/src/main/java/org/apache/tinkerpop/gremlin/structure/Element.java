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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An {@link Element} is the base class for both {@link Vertex} and {@link Edge}. An {@link Element} has an identifier
 * that must be unique to its inheriting classes ({@link Vertex} or {@link Edge}). An {@link Element} can maintain a
 * collection of {@link Property} objects.  Typically, objects are Java primitives (e.g. String, long, int, boolean,
 * etc.)
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract interface Element {

    /**
     * Gets the unique identifier for the graph {@code Element}.
     *
     * @return The id of the element
     */
    public Object id();

    /**
     * Gets the label for the graph {@code Element} which helps categorize it.
     *
     * @return The label of the element
     */
    public String label();

    /**
     * Get the graph that this element is within.
     *
     * @return the graph of this element
     */
    public Graph graph();

    /**
     * Get the keys of the properties associated with this element.
     * The default implementation iterators the properties and stores the keys into a {@link HashSet}.
     *
     * @return The property key set
     */
    public default Set<String> keys() {
        final Set<String> keys = new HashSet<>();
        this.iterators().propertyIterator().forEachRemaining(property -> keys.add(property.key()));
        return Collections.unmodifiableSet(keys);
    }

    /**
     * Get a {@link Property} for the {@code Element} given its key.
     * The default implementation calls the raw {@link Element#iterators#propertyIterator}.
     */
    public default <V> Property<V> property(final String key) {
        final Iterator<? extends Property<V>> iterator = this.iterators().propertyIterator(key);
        return iterator.hasNext() ? iterator.next() : Property.<V>empty();
    }

    /**
     * Add or set a property value for the {@code Element} given its key.
     */
    public <V> Property<V> property(final String key, final V value);

    /**
     * Get the value of a {@link Property} given it's key.
     * The default implementation calls {@link Element#property} and then returns the associated value.
     *
     * @throws NoSuchElementException if the property does not exist on the {@code Element}.
     */
    @Graph.Helper
    public default <V> V value(final String key) throws NoSuchElementException {
        final Property<V> property = this.property(key);
        return property.orElseThrow(() -> Property.Exceptions.propertyDoesNotExist(key));
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    public void remove();

    /**
     * Gets the iterators for the {@code Element}.  Iterators provide low-level access to the data associated with
     * an {@code Element} as they do not come with the overhead of {@link org.apache.tinkerpop.gremlin.process.Traversal}
     * construction.  Use iterators in places where performance is most crucial.
     */
    public Element.Iterators iterators();

    /**
     * An interface that provides access to iterators over properties of an {@code Element}, without constructing a
     * {@link org.apache.tinkerpop.gremlin.process.Traversal} object.
     */
    public interface Iterators {

        /**
         * Get the values of properties as an {@link Iterator}.
         */
        @Graph.Helper
        public default <V> Iterator<V> valueIterator(final String... propertyKeys) {
            return IteratorUtils.map(this.<V>propertyIterator(propertyKeys), property -> property.value());
        }

        /**
         * Get an {@link Iterator} of properties.
         */
        public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys);
    }

    /**
     * Common exceptions to use with an element.
     */
    public static class Exceptions {

        public static IllegalArgumentException providedKeyValuesMustBeAMultipleOfTwo() {
            return new IllegalArgumentException("The provided key/value array must be a multiple of two");
        }

        public static IllegalArgumentException providedKeyValuesMustHaveALegalKeyOnEvenIndices() {
            return new IllegalArgumentException("The provided key/value array must have a String or T on even array indices");
        }

        public static IllegalStateException propertyAdditionNotSupported() {
            return new IllegalStateException("Property addition is not supported");
        }

        public static IllegalStateException propertyRemovalNotSupported() {
            return new IllegalStateException("Property removal is not supported");
        }

        public static IllegalArgumentException labelCanNotBeNull() {
            return new IllegalArgumentException("Label can not be null");
        }

        public static IllegalArgumentException labelCanNotBeEmpty() {
            return new IllegalArgumentException("Label can not be empty");
        }

        public static IllegalArgumentException labelCanNotBeAHiddenKey(final String label) {
            return new IllegalArgumentException("Label can not be a hidden key: " + label);
        }

        public static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
            return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
        }
    }
}
