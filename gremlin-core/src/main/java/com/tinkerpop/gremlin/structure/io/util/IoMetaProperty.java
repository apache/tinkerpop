package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.MetaProperty;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoMetaProperty extends IoElement {
    public Object value;

    public static IoMetaProperty from(final MetaProperty property) {
        final IoMetaProperty metaProperty = new IoMetaProperty();
        metaProperty.value = property.value();
        metaProperty.id = property.id();
        metaProperty.label = property.label();
        metaProperty.properties = property.valueMap().next();
        metaProperty.hiddenProperties = property.hiddenValueMap().next();
        return metaProperty;
    }
}
