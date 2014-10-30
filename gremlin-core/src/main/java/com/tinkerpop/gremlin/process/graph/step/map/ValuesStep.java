package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ValuesStep<E> extends PropertiesStep<E> {

    public ValuesStep(final Traversal traversal, final boolean hidden, final String... propertyKeys) {
        super(traversal, hidden, propertyKeys);
        this.setFunction(traverser -> this.hidden ?
                (Iterator) traverser.get().iterators().hiddenValueIterator(this.propertyKeys) :
                (Iterator) traverser.get().iterators().valueIterator(this.propertyKeys));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.toString(this.propertyKeys));
    }
}
