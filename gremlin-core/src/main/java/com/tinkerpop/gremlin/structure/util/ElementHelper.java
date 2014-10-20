package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class supporting common functions for {@link com.tinkerpop.gremlin.structure.Element}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElementHelper {

    /**
     * Determine whether the Element label can be legally set. This is typically used as a pre-condition check.
     *
     * @param label the element label
     * @throws IllegalArgumentException whether the label is legal and if not, a clear reason exception is provided
     */
    public static void validateLabel(final String label) throws IllegalArgumentException {
        if (null == label)
            throw Element.Exceptions.labelCanNotBeNull();
        if (label.isEmpty())
            throw Element.Exceptions.labelCanNotBeEmpty();
        if (Graph.System.isSystem(label))
            throw Element.Exceptions.labelCanNotBeASystemKey(label);
    }

    /**
     * Check if the vertex, by ID, exists. If it does return it, else create it and return it.
     *
     * @param graph the graph to check for the existence of the vertex
     * @param id    the id of the vertex to look for
     * @param label the label of the vertex to set if the vertex does not exist
     * @return a pre-existing vertex or a newly created vertex
     */
    public static Vertex getOrAddVertex(final Graph graph, final Object id, final String label) {
        try {
            return graph.v(id);
        } catch (final NoSuchElementException e) {
            return graph.addVertex(T.id, id, T.label, label);
        }
    }

    /**
     * Determines whether the property key/value for the specified thing can be legally set. This is typically used as
     * a pre-condition check prior to setting a property.
     *
     * @param key   the key of the property
     * @param value the value of the property
     * @throws IllegalArgumentException whether the key/value pair is legal and if not, a clear reason exception
     *                                  message is provided
     */
    public static void validateProperty(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Property.Exceptions.propertyValueCanNotBeNull();
        if (null == key)
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        if (key.isEmpty())
            throw Property.Exceptions.propertyKeyCanNotBeEmpty();
        if (Graph.System.isSystem(key))
            throw Property.Exceptions.propertyKeyCanNotBeASystemKey(key);
    }

    /**
     * Determines whether a list of key/values are legal, ensuring that there are an even number of values submitted
     * and that the key values in the list of arguments are {@link String} or {@link com.tinkerpop.gremlin.structure.Element} objects.
     *
     * @param propertyKeyValues a list of key/value pairs
     * @throws IllegalArgumentException if something in the pairs is illegal
     */
    public static void legalPropertyKeyValueArray(final Object... propertyKeyValues) throws IllegalArgumentException {
        if (propertyKeyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!(propertyKeyValues[i] instanceof String) && !(propertyKeyValues[i] instanceof T))
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

    /**
     * Extracts the value of the {@link com.tinkerpop.gremlin.process.T#id} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link com.tinkerpop.gremlin.process.T#id}
     * @throws NullPointerException if the value for the {@link com.tinkerpop.gremlin.process.T#id} key is {@code null}
     */
    public static Optional<Object> getIdValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.id))
                return Optional.of(keyValues[i + 1]);
        }
        return Optional.empty();
    }

    /**
     * Remove a key from the set of key value pairs. Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements. If after removal
     * there are no values left, the key value list is returned as empty.
     */
    public static Optional<Object[]> remove(final String keyToRemove, final Object... keyValues) {
        return ElementHelper.remove((Object) keyToRemove, keyValues);
    }

    public static Optional<Object[]> remove(final T accessor, final Object... keyValues) {
        return ElementHelper.remove((Object) accessor, keyValues);
    }


    private static Optional<Object[]> remove(final Object keyToRemove, final Object... keyValues) {
        final List list = Arrays.asList(keyValues);
        final List revised = IntStream.range(0, list.size())
                .filter(i -> i % 2 == 0)
                .filter(i -> !keyToRemove.equals(list.get(i)))
                .flatMap(i -> IntStream.of(i, i + 1))
                .mapToObj(list::get)
                .collect(Collectors.toList());
        return revised.size() > 0 ? Optional.of(revised.toArray()) : Optional.empty();
    }

    /**
     * Append a key/value pair to a list of existing key/values. If the key already exists in the keyValues then
     * that value is overwritten with the provided value.
     */
    public static Object[] upsert(final Object[] keyValues, final String key, final Object val) {
        if (!getKeys(keyValues).contains(key))
            return Stream.concat(Stream.of(keyValues), Stream.of(key, val)).toArray();
        else {
            final Object[] kvs = new Object[keyValues.length];
            for (int i = 0; i < keyValues.length; i = i + 2) {
                kvs[i] = keyValues[i];
                if (keyValues[i].equals(key))
                    kvs[i + 1] = val;
                else
                    kvs[i + 1] = keyValues[i + 1];
            }

            return kvs;
        }
    }

    /**
     * Converts a set of key values to a Map.  Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements.
     */
    public static Map<String, Object> asMap(final Object... keyValues) {
        return asPairs(keyValues).stream().collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
    }

    /**
     * Convert a set of key values to a list of Pair objects.  Assumes that validations have already taken place to
     * assure that key positions contain strings and that there are an even number of elements.
     */
    public static List<Pair<String, Object>> asPairs(final Object... keyValues) {
        final List list = Arrays.asList(keyValues);
        return IntStream.range(1, list.size())
                .filter(i -> i % 2 != 0)
                .mapToObj(i -> Pair.with(list.get(i - 1).toString(), list.get(i)))
                .collect(Collectors.toList());
    }

    /**
     * Gets the list of keys from the key values.
     *
     * @param keyValues a list of key/values pairs
     */
    public static Set<String> getKeys(final Object... keyValues) {
        final Set<String> keys = new HashSet<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            keys.add(keyValues[i].toString());
        }
        return keys;
    }

    /**
     * Extracts the value of the {@link com.tinkerpop.gremlin.process.T#label} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link com.tinkerpop.gremlin.process.T#label}
     * @throws ClassCastException   if the value of the label is not a {@link String}
     * @throws NullPointerException if the value for the {@link com.tinkerpop.gremlin.process.T#label} key is {@code null}
     */
    public static Optional<String> getLabelValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                ElementHelper.validateLabel((String) keyValues[i + 1]);
                return Optional.of((String) keyValues[i + 1]);
            }
        }
        return Optional.empty();
    }

    /**
     * Assign key/value pairs as properties to an {@link com.tinkerpop.gremlin.structure.Element}.  If the value of {@link com.tinkerpop.gremlin.process.T#id} or
     * {@link com.tinkerpop.gremlin.process.T#label} is in the set of pairs, then they are ignored.
     *
     * @param element           the graph element to assign the {@code propertyKeyValues}
     * @param propertyKeyValues the key/value pairs to assign to the {@code element}
     * @throws ClassCastException       if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code element} is null
     */
    public static void attachProperties(final Element element, final Object... propertyKeyValues) {
        if (null == element)
            throw Graph.Exceptions.argumentCanNotBeNull("element");

        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                element.property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

    /**
     * Assign key/value pairs as properties to an {@link com.tinkerpop.gremlin.structure.Vertex}.  If the value of {@link com.tinkerpop.gremlin.process.T#id} or
     * {@link com.tinkerpop.gremlin.process.T#label} is in the set of pairs, then they are ignored.
     *
     * @param vertex            the vertex to assign the {@code propertyKeyValues}
     * @param propertyKeyValues the key/value pairs to assign to the {@code vertex}
     * @throws ClassCastException       if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code vertex} is null
     */
    public static void attachSingleProperties(final Vertex vertex, final Object... propertyKeyValues) {
        if (null == vertex)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                vertex.singleProperty((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

    /**
     * Retrieve the properties associated with a particular element.
     * The result is a Object[] where odd indices are String keys and even indices are the values.
     *
     * @param element                the element to retrieve properties from
     * @param includeId              include Element.ID in the key/value list
     * @param includeLabel           include Element.LABEL in the key/value list
     * @param propertiesToCopy       the properties to include with an empty list meaning copy all properties
     * @param hiddenPropertiesToCopy the hidden properties to include with an empty list meaning copy all properties
     * @return a key/value array of properties where odd indices are String keys and even indices are the values.
     */
    public static Object[] getProperties(final Element element, final boolean includeId, final boolean includeLabel, final Set<String> propertiesToCopy, final Set<String> hiddenPropertiesToCopy) {
        final List<Object> keyValues = new ArrayList<>();
        if (includeId) {
            keyValues.add(T.id);
            keyValues.add(element.id());
        }
        if (includeLabel) {
            keyValues.add(T.label);
            keyValues.add(element.label());
        }
        element.keys().forEach(key -> {
            if (propertiesToCopy.isEmpty() || propertiesToCopy.contains(key)) {
                keyValues.add(key);
                keyValues.add(element.value(key));
            }
        });
        element.hiddenKeys().forEach(key -> {
            if (hiddenPropertiesToCopy.isEmpty() || hiddenPropertiesToCopy.contains(key)) {
                final String hidden = Graph.Key.hide(key);
                keyValues.add(hidden);
                keyValues.add(element.value(hidden));
            }
        });
        return keyValues.toArray(new Object[keyValues.size()]);
    }

    /**
     * A standard method for determining if two {@link com.tinkerpop.gremlin.structure.Element} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior. This method is used for Vertex, Edge, and VertexProperty.
     *
     * @param a The first {@link com.tinkerpop.gremlin.structure.Element}
     * @param b The second {@link com.tinkerpop.gremlin.structure.Element} (as an {@link Object})
     * @return true if elements and equal and false otherwise
     * @throws IllegalArgumentException if either argument is null
     */
    public static boolean areEqual(final Element a, final Object b) {
        if (null == a)
            throw Graph.Exceptions.argumentCanNotBeNull("a");
        if (null == b)
            throw Graph.Exceptions.argumentCanNotBeNull("b");

        if (a == b)
            return true;
        if (!((a instanceof Vertex && b instanceof Vertex) ||
                (a instanceof Edge && b instanceof Edge) ||
                (a instanceof VertexProperty && b instanceof VertexProperty)))
            return false;
        return haveEqualIds(a, (Element) b);
    }

    /**
     * A standard method for determining if two {@link com.tinkerpop.gremlin.structure.VertexProperty} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a the first {@link com.tinkerpop.gremlin.structure.VertexProperty}
     * @param b the second {@link com.tinkerpop.gremlin.structure.VertexProperty}
     * @return true if equal and false otherwise
     */
    public static boolean areEqual(final VertexProperty a, final Object b) {
        return ElementHelper.areEqual((Element) a, b);
    }

    /**
     * Simply tests if the value returned from {@link com.tinkerpop.gremlin.structure.Element#id()} are {@code equal()}.
     *
     * @param a the first {@link com.tinkerpop.gremlin.structure.Element}
     * @param b the second {@link com.tinkerpop.gremlin.structure.Element}
     * @return true if ids are equal and false otherwise
     */
    public static boolean haveEqualIds(final Element a, final Element b) {
        return a.id().equals(b.id());
    }

    /**
     * A standard method for determining if two {@link com.tinkerpop.gremlin.structure.Property} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a the first {@link com.tinkerpop.gremlin.structure.Property}
     * @param b the second {@link com.tinkerpop.gremlin.structure.Property}
     * @return true if equal and false otherwise
     */
    public static boolean areEqual(final Property a, final Object b) {
        if (null == a)
            throw Graph.Exceptions.argumentCanNotBeNull("a");
        if (null == b)
            throw Graph.Exceptions.argumentCanNotBeNull("b");

        if (a == b)
            return true;
        if (!(b instanceof Property))
            return false;
        if (!a.isPresent() && !((Property) b).isPresent())
            return true;
        if (!a.isPresent() && ((Property) b).isPresent() || a.isPresent() && !((Property) b).isPresent())
            return false;
        return a.key().equals(((Property) b).key()) && a.value().equals(((Property) b).value()) && areEqual(a.element(), ((Property) b).element());

    }

    public static Map<String, Object> propertyValueMap(final Element element, final boolean getHiddens, final String... propertyKeys) {
        final Map<String, Object> values = new HashMap<>();
        if (propertyKeys.length == 0) {
            (getHiddens ? element.iterators().hiddenPropertyIterator() : element.iterators().propertyIterator()).forEachRemaining(property -> values.put(property.key(), property.value()));
        } else {
            for (final String key : propertyKeys) {
                if (!Graph.Key.isHidden(key)) {
                    element.property(getHiddens ? Graph.Key.hide(key) : key).ifPresent(v -> values.put(key, v));
                }
            }
        }
        return values;
    }

    public static Map<String, Property> propertyMap(final Element element, final boolean getHiddens, final String... propertyKeys) {
        final Map<String, Property> propertyMap = new HashMap<>();
        if (propertyKeys.length == 0) {
            (getHiddens ? element.iterators().hiddenPropertyIterator() : element.iterators().propertyIterator()).forEachRemaining(property -> propertyMap.put(property.key(), property));
        } else {
            for (final String key : propertyKeys) {
                if (!Graph.Key.isHidden(key)) {
                    final Property property = element.property(getHiddens ? Graph.Key.hide(key) : key);
                    if (property.isPresent()) propertyMap.put(key, property);
                }
            }
        }
        return propertyMap;
    }

    public static Map<String, List> vertexPropertyValueMap(final Vertex vertex, final boolean getHiddens, final String... propertyKeys) {
        final Map<String, List> valueMap = new HashMap<>();
        if (propertyKeys.length == 0) {
            (getHiddens ? vertex.iterators().hiddenPropertyIterator() : vertex.iterators().propertyIterator()).forEachRemaining(property -> {
                if (valueMap.containsKey(property.key()))
                    valueMap.get(property.key()).add(property.value());
                else {
                    final List list = new ArrayList();
                    list.add(property.value());
                    valueMap.put(property.key(), list);
                }
            });
        } else {
            for (final String key : propertyKeys) {
                if (!Graph.Key.isHidden(key)) {
                    if (valueMap.containsKey(key)) {
                        final List list = valueMap.get(key);
                        (getHiddens ? vertex.iterators().hiddenPropertyIterator(key) : vertex.iterators().propertyIterator(key)).forEachRemaining(property -> list.add(property.value()));
                    } else {
                        final List list = new ArrayList();
                        (getHiddens ? vertex.iterators().hiddenPropertyIterator(key) : vertex.iterators().propertyIterator(key)).forEachRemaining(property -> list.add(property.value()));
                        if (list.size() > 0)
                            valueMap.put(key, list);
                    }
                }
            }
        }
        return valueMap;
    }

    public static Map<String, List<VertexProperty>> vertexPropertyMap(final Vertex vertex, final boolean getHiddens, final String... propertyKeys) {
        final Map<String, List<VertexProperty>> propertyMap = new HashMap<>();
        if (null == propertyKeys || propertyKeys.length == 0) {
            (getHiddens ? vertex.iterators().hiddenPropertyIterator() : vertex.iterators().propertyIterator()).forEachRemaining(property -> {
                if (propertyMap.containsKey(property.key()))
                    propertyMap.get(property.key()).add(property);
                else {
                    final List list = new ArrayList();
                    list.add(property);
                    propertyMap.put(property.key(), list);
                }
            });
        } else {
            for (final String key : propertyKeys) {
                if (propertyMap.containsKey(key)) {
                    final List list = propertyMap.get(key);
                    (getHiddens ? vertex.iterators().hiddenPropertyIterator(key) : vertex.iterators().propertyIterator(key)).forEachRemaining(list::add);
                } else {
                    final List list = new ArrayList();
                    (getHiddens ? vertex.iterators().hiddenPropertyIterator(key) : vertex.iterators().propertyIterator(key)).forEachRemaining(list::add);
                    if (list.size() > 0)
                        propertyMap.put(key, list);
                }
            }
        }
        return propertyMap;
    }
}
