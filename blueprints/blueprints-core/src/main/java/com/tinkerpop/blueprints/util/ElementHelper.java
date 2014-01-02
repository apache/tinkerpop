package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.util.Objects;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementHelper {

    /**
     * Determines whether the property key/value for the specified thing can be legally set.
     * This is typically used as a pre-condition check prior to setting a property.
     *
     * @param key   the key of the property
     * @param value the value of the property
     * @throws IllegalArgumentException whether the key/value pair is legal and if not, a clear reason exception message is provided
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

    public static void legalKeyValues(final Object... keyValues) throws IllegalArgumentException {
        if (keyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String) && !(keyValues[i] instanceof Property.Key))
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

    public static Optional<Object> getIdValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(Property.Key.ID))
                return Optional.of(keyValues[i + 1]);
        }
        return Optional.empty();
    }

    public static Optional<String> getLabelValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(Property.Key.LABEL))
                return Optional.of(keyValues[i + 1].toString());
        }
        return Optional.empty();
    }

    public static void attachKeyValues(final Element element, final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(Property.Key.ID) && !keyValues[i].equals(Property.Key.LABEL)) {
                element.setProperty(keyValues[i].toString(), keyValues[i + 1]);
            }
        }
    }

    /**
     * A standard method for determining if two elements are equal.
     * This method should be used by any Element.equals() implementation to ensure consistent behavior.
     *
     * @param a The first element
     * @param b The second element (as an object)
     * @return Whether the two elements are equal
     */
    public static boolean areEqual(final Element a, final Object b) {
        Objects.requireNonNull(a);
        Objects.requireNonNull(b);
        if (a == b)
            return true;
        if (!((a instanceof Vertex && b instanceof Vertex) || (a instanceof Edge && b instanceof Edge)))
            return false;
        return a.getId().equals(((Element) b).getId());
    }

    /**
     * Simply tests if the element ids are equal().
     *
     * @param a the first element
     * @param b the second element
     * @return Whether the two elements have equal ids
     */
    public static boolean haveEqualIds(final Element a, final Element b) {
        return a.getId().equals(b.getId());
    }

    public static boolean areEqual(final Property a, final Object b) {
        Objects.requireNonNull(a);
        Objects.requireNonNull(b);
        if (a == b)
            return true;
        if (!(b instanceof Property))
            return false;
        if (!a.isPresent() && !((Property) b).isPresent())
            return true;
        if (!a.isPresent() && ((Property) b).isPresent() || a.isPresent() && !((Property) b).isPresent())
            return false;
        return a.getKey().equals(((Property) b).getKey()) && a.getValue().equals(((Property) b).getValue()) && areEqual(a.getElement(), ((Property) b).getElement());

    }


}
