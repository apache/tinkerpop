package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.PropertyType;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyMapStep<E> extends MapStep<Element, Map<String, E>> {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;

    public PropertyMapStep(final Traversal traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.returnType = propertyType;

        if (this.returnType.forValues()) {
            this.setFunction(traverser ->
                    traverser.get() instanceof Vertex ?
                            (Map) ElementHelper.vertexPropertyValueMap((Vertex) traverser.get(), this.returnType.forHiddens(), propertyKeys) :
                            (Map) ElementHelper.propertyValueMap(traverser.get(), this.returnType.forHiddens(), propertyKeys));
        } else {
            this.setFunction(traverser ->
                    traverser.get() instanceof Vertex ?
                            (Map) ElementHelper.vertexPropertyMap((Vertex) traverser.get(), this.returnType.forHiddens(), propertyKeys) :
                            (Map) ElementHelper.propertyMap(traverser.get(), this.returnType.forHiddens(), propertyKeys));
        }
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String toString() {
        return this.propertyKeys.length == 0 ?
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase()) :
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase(), Arrays.toString(this.propertyKeys));
    }
}
