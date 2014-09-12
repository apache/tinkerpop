package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedMetaProperty;

/**
 * Serializable form of {@link DetachedMetaProperty} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoMetaProperty extends IoElement {
    public Object value;

    public static IoMetaProperty from(final MetaProperty property) {
        if (property instanceof DetachedMetaProperty)
            throw new IllegalArgumentException(String.format("Cannot convert %s", DetachedMetaProperty.class.getSimpleName()));

        final IoMetaProperty metaProperty = new IoMetaProperty();
        metaProperty.value = property.value();
        metaProperty.id = property.id();
        metaProperty.label = property.label();
        metaProperty.properties = property.valueMap().next();
        metaProperty.hiddenProperties = property.hiddenValueMap().next();
        return metaProperty;
    }
}
