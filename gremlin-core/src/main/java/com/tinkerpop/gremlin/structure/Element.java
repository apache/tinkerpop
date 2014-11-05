package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
     * Get the keys from non-hidden properties.
     *
     * @return The non-hidden key set
     */
    public default Set<String> keys() {
        final Set<String> keys = new HashSet<>();
        this.iterators().propertyIterator().forEachRemaining(property -> keys.add(property.key()));
        return keys;
    }

    /**
     * Get the keys of hidden properties.
     *
     * @return The hidden key set
     */
    public default Set<String> hiddenKeys() {
        final Set<String> hiddenKeys = new HashSet<>();
        this.iterators().hiddenPropertyIterator().forEachRemaining(property -> hiddenKeys.add(Graph.Key.unHide(property.key())));
        return hiddenKeys;
    }

    /**
     * Get a {@link Property} for the {@code Element} given its key.  Hidden properties can be retrieved by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hide}.
     */
    public default <V> Property<V> property(final String key) {
        final Iterator<? extends Property<V>> iterator = Graph.Key.isHidden(key) ?
                this.iterators().hiddenPropertyIterator(Graph.Key.unHide(key)) :
                this.iterators().propertyIterator(key);
        return iterator.hasNext() ? iterator.next() : Property.<V>empty();
    }

    /**
     * Add or set a property value for the {@code Element} given its key.  Hidden properties can be set by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hide}.
     */
    public <V> Property<V> property(final String key, final V value);

    /**
     * Get the value of a {@link Property} given it's key.
     *
     * @throws NoSuchElementException if the property does not exist on the {@code Element}.
     */
    public default <V> V value(final String key) throws NoSuchElementException {
        final Property<V> property = this.property(key);
        return property.orElseThrow(() -> Property.Exceptions.propertyDoesNotExist(key));
    }

    public default <V> V value(final String key, final V orElse) {
        final Property<V> property = this.property(key);
        return property.orElse(orElse);
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    public void remove();

    /**
     * Gets the iterators for the {@code Element}.  Iterators provide low-level access to the data associated with
     * an {@code Element} as they do not come with the overhead of {@link com.tinkerpop.gremlin.process.Traversal}
     * construction.  Use iterators in places where performance is most crucial.
     */
    public Element.Iterators iterators();

    /**
     * An interface that provides access to iterators over properties of an {@code Element}, without constructing a
     * {@link com.tinkerpop.gremlin.process.Traversal} object.
     */
    public interface Iterators {

        /**
         * Get the values of non-hidden properties as a {@link Map} of keys and values.
         */
        public default <V> Iterator<V> valueIterator(final String... propertyKeys) {
            return StreamFactory.stream(this.propertyIterator(propertyKeys)).map(property -> (V) property.value()).iterator();
        }

        /**
         * Get the values of hidden properties as a {@link Map} of keys and values.
         */
        public default <V> Iterator<V> hiddenValueIterator(final String... propertyKeys) {
            return StreamFactory.stream(this.hiddenPropertyIterator(propertyKeys)).map(property -> (V) property.value()).iterator();
        }

        /**
         * Get an {@link Iterator} of non-hidden properties.
         */
        public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys);

        /**
         * Get an {@link Iterator} of hidden properties.
         */
        public <V> Iterator<? extends Property<V>> hiddenPropertyIterator(final String... propertyKeys);
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

        public static IllegalArgumentException labelCanNotBeASystemKey(final String label) {
            return new IllegalArgumentException("Label can not be a system key: " + label);
        }

        public static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
            return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
        }
    }
}
