package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Thing;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ThingHelper {

    /**
     * Determines whether the property key/value for the specified thing can be legally set.
     * This is typically used as a pre-condition check prior to setting a property.
     *
     * @param thing the thing for the property to be set
     * @param key   the key of the property
     * @param value the value of the property
     * @throws IllegalArgumentException whether the triple is legal and if not, a clear reason message is provided
     */
    public static void validateProperty(final Thing thing, final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Property.Features.propertyValueCanNotBeNull();
        if (null == key)
            throw Property.Features.propertyKeyCanNotBeNull();
        if (key.equals(StringFactory.ID))
            throw Property.Features.propertyKeyIdIsReserved();
        if (thing instanceof Edge && key.equals(StringFactory.LABEL))
            throw Property.Features.propertyKeyLabelIsReservedForEdges();
        if (key.isEmpty())
            throw Property.Features.propertyKeyCanNotBeEmpty();
    }
}
