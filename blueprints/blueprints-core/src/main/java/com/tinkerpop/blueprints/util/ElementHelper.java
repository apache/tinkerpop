package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.util.Optional;

/**
 * Utility class supporting common functions for {@link Element}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElementHelper {

    /**
     * Determines whether the property key/value for the specified thing can be legally set. This is typically used as
     * a pre-condition check prior to setting a property.
     *
     * @param key   the key of the property
     * @param value the value of the property
     * @throws IllegalArgumentException whether the key/value pair is legal and if not, a clear reason exception
     *         message is provided
     */
    public static void validateProperty(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Property.Exceptions.propertyValueCanNotBeNull();
        if (null == key)
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        if (key.equals(Property.Key.ID))
            throw Property.Exceptions.propertyKeyIdIsReserved();
        if (key.equals(Property.Key.LABEL))
            throw Property.Exceptions.propertyKeyLabelIsReserved();
        if (key.isEmpty())
            throw Property.Exceptions.propertyKeyCanNotBeEmpty();
    }

    /**
     * Determines whether a list of key/values are legal, ensuring that there are an even number of values submitted
     * and that the key values in the list of arguments are {@link String} or {@link Property.Key} objects.
     *
     * @param keyValues a list of key/value pairs
     * @throws IllegalArgumentException if something in the pairs is illegal
     */
    public static void legalKeyValues(final Object... keyValues) throws IllegalArgumentException {
        if (keyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String) && !(keyValues[i] instanceof Property.Key))
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

    /**
     * Extracts the value of the {@link Property.Key#ID} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link Property.Key#ID}
     * @throws NullPointerException if the value for the {@link Property.Key#ID} key is {@code null}
     */
    public static Optional<Object> getIdValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(Property.Key.ID))
                return Optional.of(keyValues[i + 1]);
        }
        return Optional.empty();
    }

    /**
     * Extracts the value of the {@link Property.Key#LABEL} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link Property.Key#LABEL}
     * @throws ClassCastException if the value of the label is not a {@link String}
     * @throws NullPointerException if the value for the {@link Property.Key#LABEL} key is {@code null}
     */
    public static Optional<String> getLabelValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(Property.Key.LABEL))
                return Optional.of((String) keyValues[i + 1]);
        }
        return Optional.empty();
    }

    /**
     * Assign key/value pairs as properties to an {@link Element}.  If the value of {@link Property.Key#ID} or
     * {@link Property.Key#LABEL} is in the set of pairs, then they are ignored.
     *
     * @param element the graph element to assign the {@code keyValues}
     * @param keyValues the key/value pairs to assign to the {@code element}
     * @throws ClassCastException if the value of the key is not a {@link String}
     * @throws IllegalArgumentException if the value of {@code element} is null
     */
    public static void attachKeyValues(final Element element, final Object... keyValues) {
        if (null == element)
            throw Graph.Exceptions.argumentCanNotBeNull("element");

        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(Property.Key.ID) && !keyValues[i].equals(Property.Key.LABEL))
                element.setProperty((String) keyValues[i], keyValues[i + 1]);
        }
    }

    /**
     * A standard method for determining if two {@link Element} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a The first {@link Element}
     * @param b The second {@link Element} (as an {@link Object})
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
        if (!((a instanceof Vertex && b instanceof Vertex) || (a instanceof Edge && b instanceof Edge)))
            return false;
        return haveEqualIds(a, (Element) b);
    }

    /**
     * Simply tests if the value returned from {@link com.tinkerpop.blueprints.Element#getId()} are {@code equal()}.
     *
     * @param a the first {@link Element}
     * @param b the second {@link Element}
     * @return true if ids are equal and false otherwise
     */
    public static boolean haveEqualIds(final Element a, final Element b) {
        return a.getId().equals(b.getId());
    }

    /**
     * A standard method for determining if two {@link Property} objects are equal. This method should be used by any
     * {@link Object#equals(Object)} implementation to ensure consistent behavior.
     *
     * @param a the first {@link Property}
     * @param b the second {@link Property}
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
        return a.getKey().equals(((Property) b).getKey()) && a.get().equals(((Property) b).get()) && areEqual(a.getElement(), ((Property) b).getElement());

    }


}
