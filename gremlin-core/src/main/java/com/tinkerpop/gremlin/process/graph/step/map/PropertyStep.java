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

    public PropertyStep(final Traversal traversal, final String key) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().property(key));
    }

    public void reverse() {
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }
}
