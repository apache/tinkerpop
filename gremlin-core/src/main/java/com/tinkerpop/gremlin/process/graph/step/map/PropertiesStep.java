package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertiesStep<E> extends FlatMapStep<Element, E> implements Reversible {

    protected final String[] propertyKeys;
    protected final boolean getHiddens;
    protected final boolean getValues;

    public PropertiesStep(final Traversal traversal, final boolean getHiddens, final boolean getValues, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.getHiddens = getHiddens;
        this.getValues = getValues;
        if (this.getValues) {
            this.setFunction(traverser -> this.getHiddens ?
                    (Iterator) traverser.get().iterators().hiddenValueIterator(this.propertyKeys) :
                    (Iterator) traverser.get().iterators().valueIterator(this.propertyKeys));

        } else {
            this.setFunction(traverser -> this.getHiddens ?
                    (Iterator) traverser.get().iterators().hiddenPropertyIterator(this.propertyKeys) :
                    (Iterator) traverser.get().iterators().propertyIterator(this.propertyKeys));
        }
    }

    @Override
    public void reverse() {
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }

    @Override
    public String toString() {
        return this.propertyKeys.length == 0 ? super.toString() : TraversalHelper.makeStepString(this, Arrays.toString(this.propertyKeys));
    }

    public boolean isGettingHiddens() {
        return this.getHiddens;
    }

    public boolean isGettingValues() {
        return this.getValues;
    }
}
