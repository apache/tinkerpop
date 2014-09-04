package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HiddenMapStep<E> extends MapStep<Element, Map<String, ? extends Property<E>>> {

    public String[] propertyKeys;

    public HiddenMapStep(final Traversal traversal, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        for (int i = 0; i < propertyKeys.length; i++) {
            this.propertyKeys[i] = Graph.Key.hide(this.propertyKeys[i]);
        }
        this.setFunction(traverser ->
                traverser.get() instanceof Vertex ?
                        (Map) ElementHelper.metaPropertyMap((Vertex) traverser.get(), propertyKeys) :
                        (Map) ElementHelper.propertyMap(traverser.get(), propertyKeys));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.asList(this.propertyKeys));
    }
}
