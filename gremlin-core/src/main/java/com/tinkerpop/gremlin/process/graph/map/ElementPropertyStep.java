package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPropertyStep<E> extends MapStep<Element, Property<E>> implements Reversible {

    public ElementPropertyStep(final Traversal traversal, final String key) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().property(key));
    }

    public void reverse() {
        final int stepIndex = TraversalHelper.removeStep(this, this.traversal);
        TraversalHelper.insertStep(new PropertyElementStep(this.traversal), stepIndex, this.traversal);
    }
}
