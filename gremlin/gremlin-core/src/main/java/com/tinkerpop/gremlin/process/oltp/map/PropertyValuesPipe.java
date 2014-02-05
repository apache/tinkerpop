package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValuesPipe extends MapPipe<Element, Map<String, Object>> {

    public PropertyValuesPipe(final Traversal pipeline, final String... keys) {
        super(pipeline, holder -> ElementHelper.propertyMap(holder.get(), keys));
    }
}
