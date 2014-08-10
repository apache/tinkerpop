package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuesStep extends MapStep<Element, Map<String, Object>> {

    public ValuesStep(final Traversal traversal, final String... keys) {
        super(traversal);
        this.setFunction(traverser -> ElementHelper.propertyMap(traverser.get(), keys));
    }
}
