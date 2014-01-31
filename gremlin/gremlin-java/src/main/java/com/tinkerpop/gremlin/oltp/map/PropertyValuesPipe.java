package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValuesPipe extends MapPipe<Element, Map<String, Object>> {

    public PropertyValuesPipe(final Pipeline pipeline, final String... keys) {
        super(pipeline, holder -> ElementHelper.propertyMap(holder.get(), keys));
    }
}
