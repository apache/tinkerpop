package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.strategy.GraphComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.GraphStandardStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traversal<S, E> extends Iterator<E>, Cloneable {

    public static final String OF = "of";

    public SideEffects sideEffects();

    public Strategies strategies();

    public void addStarts(final Iterator<Traverser<S>> starts);

    public void addStart(final Traverser<S> start);

    public default <E2> Traversal<S, E2> addStep(final Step<?, E2> step) {
        TraversalHelper.insertStep(step, this);
        return (Traversal) this;
    }

    public List<Step> getSteps();

    public default void prepareForGraphComputer() {
        this.sideEffects().removeGraph();
        this.strategies().unregister(TraverserSourceStrategy.class);
        this.strategies().unregister(GraphStandardStrategy.class);
        this.strategies().register(GraphComputerStrategy.instance());
    }

    public default Traversal<S, E> submit(final GraphComputer computer) {
        try {
            this.prepareForGraphComputer();
            this.strategies().apply();
            final TraversalVertexProgram vertexProgram = TraversalVertexProgram.build().traversal(this::clone).create();
            final ComputerResult result = computer.program(vertexProgram).submit().get();
            final GraphTraversal<S, S> traversal = result.graph().of();
            return traversal.addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public default void reset() {
        this.getSteps().forEach(Step::reset);
    }

    /**
     * Cloning is used to duplicate the traversal typically in OLAP environments.
     *
     * @return The cloned traversal
     */
    public Traversal<S, E> clone();

    public interface Strategies {

        public List<TraversalStrategy> toList();

        public void register(final TraversalStrategy traversalStrategy);

        public void unregister(final Class<? extends TraversalStrategy> optimizerClass);

        public void clear();

        public void apply();

        public boolean complete();
    }

    public interface SideEffects {

        public static final String SIDE_EFFECTS = Graph.Key.hide("gremlin.sideEffects");
        public static final String GRAPH_KEY = Graph.System.system("g");

        /**
         * Determines if the {@link Traversal.SideEffects} contains the respective key.
         * If the key references a stored {@link Supplier}, then it should return true as it will be dynamically created on get().
         *
         * @param key the key to check for
         * @return whether the key exists in the sideEffects
         */
        public default boolean exists(final String key) {
            return this.keys().contains(key);
        }

        /**
         * Set the specified key to the specified value.
         * If a {@link Supplier} is provided, it is NOT assumed to be a supplier as set by registerSupplier().
         *
         * @param key   the key
         * @param value the value
         */
        public void set(final String key, final Object value);

        /**
         * Get the sideEffect associated with the provided key.
         * If the sideEffect contains an object for the key, return it.
         * Else if the sideEffect has a registered {@link Supplier} for that key, generate the object, store the object in the sideEffects, and return it.
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
         * If a value or registered {@link Supplier} exists for the provided key, consume it with the provided consumer.
         *
         * @param key      the key to the value
         * @param consumer the consumer to process the value
         * @param <V>      the type of the value to consume
         */
        public default <V> void ifPresent(final String key, final Consumer<V> consumer) {
            if (this.exists(key)) consumer.accept(this.get(key));
        }

        /**
         * Remove both the value and registered {@link Supplier} associated with provided key.
         *
         * @param key the key of the value and registered supplier to remove
         */
        public void remove(final String key);

        /**
         * The keys of the sideEffect which includes registered {@link Supplier} keys.
         * In essence, that which is possible to get().
         *
         * @return the keys of the sideEffect
         */
        public Set<String> keys();

        ////////////

        /**
         * Register a {@link Supplier} with the provided key.
         * When sideEffects get() are called, if no object exists and there exists a registered supplier for the key, the object is generated.
         * Registered suppliers are used for the lazy generation of sideEffect data.
         *
         * @param key      the key to register the supplier with
         * @param supplier the supplier that will generate an object when get() is called if it hasn't already been created
         */
        public void registerSupplier(final String key, final Supplier supplier);

        /**
         * Get the registered {@link Supplier} associated with the specified key.
         *
         * @param key the key associated with the supplier
         * @param <V> The object type of the supplier
         * @return A non-empty optional if the supplier exists
         */
        public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key);

        /**
         * A helper method to register a {@link Supplier} if it has not already been registered.
         *
         * @param key the key of the supplier to register
         * @param supplier the supplier to register if the key has not already been registered
         */
        public default void registerSupplierIfAbsent(final String key, final Supplier supplier) {
            if (!this.getRegisteredSupplier(key).isPresent())
                this.registerSupplier(key, supplier);
        }

        /**
         * If the sideEffect contains an object associated with the key, return it.
         * Else if a "with" supplier exists for the key, generate the object, store it in the sideEffects and return the object.
         * Else use the provided supplier to generate the object, store it in the sideEffects and return the object.
         * Note that if the orCreate supplier is used, it is NOT registered as a {@link Supplier}.
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

        public default boolean graphExists() {
            return this.exists(GRAPH_KEY);
        }

        public default void setGraph(final Graph graph) {
            this.set(GRAPH_KEY, graph);
        }

        public default Graph getGraph() {
            if (this.exists(GRAPH_KEY))
                return this.<Graph>get(GRAPH_KEY);
            else
                throw new IllegalStateException("There is no graph stored in these side effects");
        }

        public default void removeGraph() {
            this.remove(GRAPH_KEY);
        }

        ////////////

        public default <V> void forEach(final BiConsumer<String, V> biConsumer) {
            this.keys().forEach(key -> biConsumer.accept(key, this.get(key)));
        }

        /**
         * In a distributed {@link GraphComputer} traversal, the sideEffects of the traversal are not a single object within a single JVM.
         * Instead, the sideEffects are distributed across the graph and the pieces are stored on the computing vertices.
         * This method is necessary to call when the {@link Traversal} is processing the {@link Traverser}s at a particular {@link Vertex}.
         *
         * @param vertex the vertex where the traversal is currently executing.
         */
        public void setLocalVertex(final Vertex vertex);

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

    /////////

    public default Traversal<S, E> reverse() {
        this.getSteps().stream().filter(step -> step instanceof Reversible).forEach(step -> ((Reversible) step).reverse());
        return this;
    }

    public default List<E> next(final int amount) {   // TODO: Use bulk (same theory as range)
        final List<E> result = new ArrayList<>();
        int counter = 0;
        while (counter++ < amount && this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    public default List<E> toList() {
        return (List<E>) this.fill(new ArrayList<>());
    }

    public default Set<E> toSet() {
        return (Set<E>) this.fill(new HashSet<>());
    }

    public default Collection<E> fill(final Collection<E> collection) {
        try {
            this.strategies().apply();
            // use the end step so the results are bulked
            final Step<?, E> endStep = TraversalHelper.getEnd(this);
            while (true) {
                final Traverser<E> traverser = endStep.next();
                TraversalHelper.addToCollection(collection, traverser.get(), traverser.bulk());
            }
        } catch (final NoSuchElementException ignored) {
        }
        return collection;
    }

    public default Traversal iterate() {
        try {
            this.strategies().apply();
            // use the end step so the results are bulked
            final Step<?, E> endStep = TraversalHelper.getEnd(this);
            while (true) {
                endStep.next();
            }
        } catch (final NoSuchElementException ignored) {
        }
        return this;
    }

    public default void forEach(final Consumer<E> consumer) {
        try {
            while (this.hasNext()) {
                consumer.accept(this.next());
            }
        } catch (final NoSuchElementException ignored) {

        }
    }
}
