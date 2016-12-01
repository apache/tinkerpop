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
package org.apache.tinkerpop.gremlin.process.traversal;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@link Traversal} can maintain global sideEffects.
 * Unlike {@link Traverser} "sacks" which are local sideEffects, TraversalSideEffects are accessible by all {@link Traverser} instances within the {@link Traversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalSideEffects extends Cloneable, Serializable, AutoCloseable {

    /**
     * Return true if the key is a registered side-effect.
     *
     * @param key the key to check for existence
     * @return whether the key exists or not
     */
    public default boolean exists(final String key) {
        return this.keys().contains(key);
    }

    /**
     * Get the sideEffect associated with the provided key.
     * If the sideEffect contains an object for the key, return it.
     * Else if the sideEffect has a registered {@link java.util.function.Supplier} for that key, generate the object, store the object in the sideEffects, and return it.
     *
     * @param key the key to get the value for
     * @param <V> the type of the value to retrieve
     * @return the value associated with key
     * @throws IllegalArgumentException if the key does not reference an object or a registered supplier.
     */
    public <V> V get(final String key) throws IllegalArgumentException;

    /**
     * Set the specified key to the specified value.
     * This method should not be used in a distributed environment. Instead, use {@link TraversalSideEffects#add(String, Object)}.
     * This method is only safe when there is only one representation of the side-effect and thus, not distributed across threads or machines.
     *
     * @param key   the key they key of the side-effect
     * @param value the value the new value for the side-effect
     * @throws IllegalArgumentException if the key does not reference a registered side-effect.
     */
    public void set(final String key, final Object value) throws IllegalArgumentException;

    /**
     * Remove both the value and registered {@link java.util.function.Supplier} associated with provided key.
     *
     * @param key the key of the value and registered supplier to remove
     */
    public void remove(final String key);

    /**
     * The keys of the sideEffect which includes registered {@link java.util.function.Supplier} keys.
     * In essence, that which is possible to get().
     *
     * @return the keys of the sideEffect
     */
    public Set<String> keys();

    /**
     * Invalidate the side effect cache for traversal.
     */
    public default void close() throws Exception {
        // do nothing
    }

    /**
     * Determines if there are any side-effects to be retrieved.
     */
    public default boolean isEmpty() {
        return keys().size() == 0;
    }

    /**
     * Register a side-effect with the {@link TraversalSideEffects} providing a {@link Supplier} and a {@link BinaryOperator}.
     * If a null value is provided for the supplier or reducer, then it no supplier or reducer is registered.
     *
     * @param key          the key of the side-effect value
     * @param initialValue the initial value supplier
     * @param reducer      the reducer to use for merging a distributed side-effect value into a single value
     * @param <V>          the type of the side-effect value
     */
    public <V> void register(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer);

    /**
     * Register a side-effect with the {@link TraversalSideEffects} providing a {@link Supplier} and a {@link BinaryOperator}.
     * The registration will only overwrite a supplier or reducer if no supplier or reducer existed prior.
     * If a null value is provided for the supplier or reducer, then it no supplier or reducer is registered.
     *
     * @param key          the key of the side-effect value
     * @param initialValue the initial value supplier
     * @param reducer      the reducer to use for merging a distributed side-effect value into a single value
     * @param <V>          the type of the side-effect value
     */
    public <V> void registerIfAbsent(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer);

    /**
     * Get the reducer associated with the side-effect key. If no reducer was registered, then {@link Operator#assign} is provided.
     *
     * @param key the key of the side-effect
     * @param <V> the type of the side-effect value
     * @return the registered reducer
     * @throws IllegalArgumentException if no side-effect exists for the provided key
     */
    public <V> BinaryOperator<V> getReducer(final String key) throws IllegalArgumentException;

    /**
     * Get the supplier associated with the side-effect key. If no supplier was registered, then {@link org.apache.tinkerpop.gremlin.util.function.ConstantSupplier} is provided.
     *
     * @param key the key of the side-effect
     * @param <V> the type of the side-effect value
     * @return the registered supplier
     * @throws IllegalArgumentException if no side-effect exists for the provided key
     */
    public <V> Supplier<V> getSupplier(final String key) throws IllegalArgumentException;

    /**
     * Add a value to the global side-effect value.
     * This should be used by steps to ensure that side-effects are merged properly in a distributed environment.
     * {@link TraversalSideEffects#set(String, Object)}  should only be used in single-threaded systems or by a master traversal in a distributed environment.
     *
     * @param key   the key of the side-effect.
     * @param value the partital value (to be merged) of the side-effect.
     * @throws IllegalArgumentException if no side-effect exists for the provided key
     */
    public void add(final String key, final Object value) throws IllegalArgumentException;

    /**
     * Set the initial value of each {@link Traverser} "sack" along with the operators for splitting and merging sacks.
     * If no split operator is provided, then a direct memory copy is assumed (this is typically good for primitive types and strings).
     * If no merge operator is provided, then traversers with sacks will not be merged.
     *
     * @param initialValue  the initial value supplier of the traverser sack
     * @param splitOperator the split operator for splitting traverser sacks
     * @param mergeOperator the merge operator for merging traverser sacks
     * @param <S>           the sack type
     */
    public <S> void setSack(final Supplier<S> initialValue, final UnaryOperator<S> splitOperator, final BinaryOperator<S> mergeOperator);

    /**
     * If sacks are enabled, get the initial value of the {@link Traverser} sack.
     * If its not enabled, then <code>null</code> is returned.
     *
     * @param <S> the sack type
     * @return the supplier of the initial value of the traverser sack
     */
    public <S> Supplier<S> getSackInitialValue();

    /**
     * If sacks are enabled and a split operator has been specified, then get it (else get <code>null</code>).
     * The split operator is used to split a sack when a bifurcation in a {@link Traverser} happens.
     *
     * @param <S> the sack type
     * @return the operator for splitting a traverser sack
     */
    public <S> UnaryOperator<S> getSackSplitter();

    /**
     * If sacks are enabled and a merge function has been specified, then get it (else get <code>null</code>).
     * The merge function is used to merge two sacks when two {@link Traverser}s converge.
     *
     * @param <S> the sack type
     * @return the operator for merging two traverser sacks
     */
    public <S> BinaryOperator<S> getSackMerger();

    ////////////

    public default <V> void forEach(final BiConsumer<String, V> biConsumer) {
        this.keys().forEach(key -> biConsumer.accept(key, this.<V>get(key)));
    }

    /**
     * Cloning is used to duplicate the sideEffects typically in distributed execution environments.
     *
     * @return The cloned sideEffects
     */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public TraversalSideEffects clone();

    /**
     * Add the current {@link TraversalSideEffects} values, suppliers, and reducers to the provided {@link TraversalSideEffects}.
     * The implementation should (under the hood), use {@link TraversalSideEffects#registerIfAbsent(String, Supplier, BinaryOperator)} so that
     * if the argument {@link TraversalSideEffects} already has a registered supplier or binary operator, then don't overwrite it.
     *
     * @param sideEffects the sideEffects to add this traversal's sideEffect data to.
     */
    public void mergeInto(final TraversalSideEffects sideEffects);

    public static class Exceptions {

        public static IllegalArgumentException sideEffectKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Side-effect key can not be the empty string");
        }

        public static IllegalArgumentException sideEffectKeyCanNotBeNull() {
            return new IllegalArgumentException("Side-effect key can not be null");
        }

        public static IllegalArgumentException sideEffectValueCanNotBeNull() {
            return new IllegalArgumentException("Side-effect value can not be null");
        }

        public static IllegalArgumentException sideEffectKeyDoesNotExist(final String key) {
            return new IllegalArgumentException("The side-effect key does not exist in the side-effects: " + key);
        }
    }

    /////////////////// DEPRECATED METHOD SINCE 3.2.0

    /**
     * Register a {@link java.util.function.Supplier} with the provided key.
     * When sideEffects get() are called, if no object exists and there exists a registered supplier for the key, the object is generated.
     * Registered suppliers are used for the lazy generation of sideEffect data.
     *
     * @param key      the key to register the supplier with
     * @param supplier the supplier that will generate an object when get() is called if it hasn't already been created
     * @deprecated As of release 3.2.0, replaced by {@link TraversalSideEffects#register(String, Supplier, BinaryOperator)}.
     */
    @Deprecated
    public void registerSupplier(final String key, final Supplier supplier);


    /**
     * A helper method to register a {@link java.util.function.Supplier} if it has not already been registered.
     *
     * @param key      the key of the supplier to register
     * @param supplier the supplier to register if the key has not already been registered
     * @deprecated As of release 3.2.0, replaced by {@link TraversalSideEffects#registerIfAbsent(String, Supplier, BinaryOperator)}.
     */
    @Deprecated
    public default void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        if (!this.getRegisteredSupplier(key).isPresent())
            this.registerSupplier(key, supplier);
    }

    /**
     * Get the registered {@link java.util.function.Supplier} associated with the specified key.
     *
     * @param key the key associated with the supplier
     * @param <V> The object type of the supplier
     * @return A non-empty optional if the supplier exists
     * @deprecated As of release 3.2.0, replaced by {@link TraversalSideEffects#getSupplier(String)}.
     */
    @Deprecated
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key);


    /**
     * If the sideEffect contains an object associated with the key, return it.
     * Else if a "with" supplier exists for the key, generate the object, store it in the sideEffects and return the object.
     * Else use the provided supplier to generate the object, store it in the sideEffects and return the object.
     * Note that if the orCreate supplier is used, it is NOT registered as a {@link java.util.function.Supplier}.
     *
     * @param key      the key of the object to get
     * @param orCreate if the object doesn't exist as an object or suppliable object, then generate it with the specified supplier
     * @param <V>      the return type of the object
     * @return the object that is either retrieved, generated, or supplier via orCreate
     * @deprecated As of release 3.2.0, replaced by {@link TraversalSideEffects#register(String, Supplier, BinaryOperator)} to register side-effects.
     */
    @Deprecated
    public default <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        final V value = this.exists(key) ? this.get(key) : null;
        if (null != value)
            return value;
        final Optional<Supplier<V>> with = this.getRegisteredSupplier(key);
        if (with.isPresent()) {
            final V v = with.get().get();
            this.set(key, v);
            return v;
        } else {
            final V v = orCreate.get();
            this.set(key, v);
            return v;
        }
    }


    ///////////////////
}
