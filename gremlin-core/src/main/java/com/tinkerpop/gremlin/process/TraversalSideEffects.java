package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalSideEffects extends Cloneable {

    public static final String SIDE_EFFECTS = "gremlin.traversal.sideEffects";

    /**
     * Determines if the {@link TraversalSideEffects} contains the respective key.
     * If the key references a stored {@link java.util.function.Supplier}, then it should return true as it will be dynamically created on get().
     *
     * @param key the key to check for
     * @return whether the key exists in the sideEffects
     */
    public default boolean exists(final String key) {
        return this.keys().contains(key);
    }

    /**
     * Set the specified key to the specified value.
     * If a {@link java.util.function.Supplier} is provided, it is NOT assumed to be a supplier as set by registerSupplier().
     *
     * @param key   the key
     * @param value the value
     */
    public void set(final String key, final Object value);

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
     * Return the value associated with the key or return the provided otherValue.
     * The otherValue will not be stored in the sideEffect.
     *
     * @param key        the key to get the value for
     * @param otherValue if not value is associated with key, return the other value.
     * @param <V>        the type of the value to get
     * @return the value associated with the key or the otherValue
     */
    public default <V> V orElse(final String key, final V otherValue) {
        return this.exists(key) ? this.get(key) : otherValue;
    }

    /**
     * If a value or registered {@link java.util.function.Supplier} exists for the provided key, consume it with the provided consumer.
     *
     * @param key      the key to the value
     * @param consumer the consumer to process the value
     * @param <V>      the type of the value to consume
     */
    public default <V> void ifPresent(final String key, final Consumer<V> consumer) {
        if (this.exists(key)) consumer.accept(this.get(key));
    }

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

    ////////////

    /**
     * Register a {@link java.util.function.Supplier} with the provided key.
     * When sideEffects get() are called, if no object exists and there exists a registered supplier for the key, the object is generated.
     * Registered suppliers are used for the lazy generation of sideEffect data.
     *
     * @param key      the key to register the supplier with
     * @param supplier the supplier that will generate an object when get() is called if it hasn't already been created
     */
    public void registerSupplier(final String key, final Supplier supplier);

    /**
     * Get the registered {@link java.util.function.Supplier} associated with the specified key.
     *
     * @param key the key associated with the supplier
     * @param <V> The object type of the supplier
     * @return A non-empty optional if the supplier exists
     */
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key);

    /**
     * A helper method to register a {@link java.util.function.Supplier} if it has not already been registered.
     *
     * @param key      the key of the supplier to register
     * @param supplier the supplier to register if the key has not already been registered
     */
    public default void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        if (!this.getRegisteredSupplier(key).isPresent())
            this.registerSupplier(key, supplier);
    }

    public <S> void setSack(final Supplier<S> initialValue, final Optional<UnaryOperator<S>> splitOperator);

    public <S> Optional<Supplier<S>> getSackInitialValue();

    public <S> Optional<UnaryOperator<S>> getSackSplitOperator();

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
     */
    public default <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        if (this.exists(key))
            return this.<V>get(key);
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

    ////////////

    public default <V> void forEach(final BiConsumer<String, V> biConsumer) {
        this.keys().forEach(key -> biConsumer.accept(key, this.get(key)));
    }

    /**
     * In a distributed {@link com.tinkerpop.gremlin.process.computer.GraphComputer} traversal, the sideEffects of the traversal are not a single object within a single JVM.
     * Instead, the sideEffects are distributed across the graph and the pieces are stored on the computing vertices.
     * This method is necessary to call when the {@link com.tinkerpop.gremlin.process.Traversal} is processing the {@link com.tinkerpop.gremlin.process.Traverser}s at a particular {@link com.tinkerpop.gremlin.structure.Vertex}.
     *
     * @param vertex the vertex where the traversal is currently executing.
     */
    public void setLocalVertex(final Vertex vertex);

    /**
     * Cloning is used to duplicate the sideEffects typically in OLAP environments.
     *
     * @return The cloned sideEffects
     */
    public TraversalSideEffects clone() throws CloneNotSupportedException;

    /**
     * Add the current {@link TraversalSideEffects} data and suppliers to the provided {@link TraversalSideEffects}.
     *
     * @param sideEffects the sideEffects to add this traversal's sideEffect data to.
     */
    public void mergeInto(final TraversalSideEffects sideEffects);

    public static class Exceptions {

        public static IllegalArgumentException sideEffectKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Side effect key can not be the empty string");
        }

        public static IllegalArgumentException sideEffectKeyCanNotBeNull() {
            return new IllegalArgumentException("Side effect key can not be null");
        }

        public static IllegalArgumentException sideEffectValueCanNotBeNull() {
            return new IllegalArgumentException("Side effect value can not be null");
        }

        public static IllegalArgumentException sideEffectDoesNotExist(final String key) {
            return new IllegalArgumentException("Side effects do not have a value for provided key: " + key);
        }

        public static UnsupportedOperationException dataTypeOfSideEffectValueNotSupported(final Object val) {
            return new UnsupportedOperationException(String.format("Side effect value [%s] is of type %s is not supported", val, val.getClass()));
        }
    }
}
