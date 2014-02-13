package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValuesStep extends MapStep<Element, Map<String, Object>> {

    public PropertyValuesStep(final Traversal traversal, final String... keys) {
        super(traversal, holder -> ElementHelper.propertyMap(holder.get(), keys));
    }
}
