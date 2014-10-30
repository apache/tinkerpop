package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PropertyMapStep<E> extends MapStep<Element, Map<String, E>> {

    private final String[] propertyKeys;
    private final boolean hidden;

    public PropertyMapStep(final Traversal traversal, final boolean hidden, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.hidden = hidden;
        this.setFunction(traverser ->
                traverser.get() instanceof Vertex ?
                        (Map) ElementHelper.vertexPropertyMap((Vertex) traverser.get(), this.hidden, propertyKeys) :
                        (Map) ElementHelper.propertyMap(traverser.get(), this.hidden, propertyKeys));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.toString(this.propertyKeys));
    }
}
