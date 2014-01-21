package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Pipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuesPipe extends MapPipe<Element, Map<String, Object>> {

    public ValuesPipe(final Pipeline pipeline, final String... keys) {
        super(pipeline, holder -> {
            final Map<String, Object> values = new HashMap<>();
            final Element element = holder.get();
            if (null == keys || keys.length == 0) {
                element.getPropertyKeys().forEach(key -> values.put(key, element.getValue(key)));
            } else {
                for (final String key : keys) {
                    if (key.equals(Property.Key.ID))
                        values.put(Property.Key.ID, element.getId());
                    else if (key.equals(Property.Key.LABEL))
                        values.put(Property.Key.LABEL, element.getLabel());
                    else
                        element.getProperty(key).ifPresent(v -> values.put(key, v));
                }
            }
            return values;
        });
    }
}
