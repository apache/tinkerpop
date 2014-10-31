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
public class PropertyMapStep<E> extends MapStep<Element, Map<String, E>> {

    protected final String[] propertyKeys;
    protected final boolean getHiddens;
    protected final boolean getValues;

    public PropertyMapStep(final Traversal traversal, final boolean getHiddens, final boolean getValues, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.getHiddens = getHiddens;
        this.getValues = getValues;
        if (this.getValues) {
            this.setFunction(traverser ->
                    traverser.get() instanceof Vertex ?
                            (Map) ElementHelper.vertexPropertyValueMap((Vertex) traverser.get(), this.getHiddens, propertyKeys) :
                            (Map) ElementHelper.propertyValueMap(traverser.get(), this.getHiddens, propertyKeys));
        } else {
            this.setFunction(traverser ->
                    traverser.get() instanceof Vertex ?
                            (Map) ElementHelper.vertexPropertyMap((Vertex) traverser.get(), this.getHiddens, propertyKeys) :
                            (Map) ElementHelper.propertyMap(traverser.get(), this.getHiddens, propertyKeys));
        }
    }

    public boolean isGettingHiddens() {
        return this.getHiddens;
    }

    public boolean isGettingValues() {
        return this.getValues;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.toString(this.propertyKeys));
    }
}
