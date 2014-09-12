package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedMetaProperty;

import java.util.HashMap;

/**
 * Serializable form of {@link DetachedMetaProperty} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoMetaProperty extends IoElement {
    public Object value;

    public static IoMetaProperty from(final MetaProperty property) {
        final IoMetaProperty metaProperty = new IoMetaProperty();
        metaProperty.value = property.value();
        metaProperty.id = property.id();
        metaProperty.label = property.label();
        metaProperty.properties = new HashMap();
        property.iterators().properties().forEachRemaining(p -> metaProperty.properties.put(p.key(), p.value()));
        metaProperty.hiddenProperties = new HashMap();
        property.iterators().properties().forEachRemaining(p -> metaProperty.hiddenProperties.put(p.key(), p.value()));
        return metaProperty;
    }
}
