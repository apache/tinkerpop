package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HiddensStep<E> extends FlatMapStep<Element, Property<E>> implements Reversible {

    private final String[] propertyKeys;

    public HiddensStep(final Traversal traversal, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.setFunction(traverser -> (Iterator) traverser.get().iterators().hiddenPropertyIterator(this.propertyKeys));
    }

    @Override
    public void reverse() {
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }

    public String toString() {
        return this.propertyKeys.length == 0 ? super.toString() : TraversalHelper.makeStepString(this, Arrays.toString(this.propertyKeys));
    }
}
