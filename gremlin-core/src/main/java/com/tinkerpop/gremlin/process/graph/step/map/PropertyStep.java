package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyStep<E> extends MapStep<Element, Property<E>> implements Reversible {

    public String propertyKey;

    public PropertyStep(final Traversal traversal, final String propertyKey) {
        super(traversal);
        this.propertyKey = propertyKey;
        this.setFunction(traverser -> traverser.get().property(this.propertyKey));
    }

    public void reverse() {
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.propertyKey);
    }
}
