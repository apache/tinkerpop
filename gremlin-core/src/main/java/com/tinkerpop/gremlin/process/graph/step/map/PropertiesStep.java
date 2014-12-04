package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.LocallyTraversable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.PropertyType;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertiesStep<E> extends FlatMapStep<Element, E> implements Reversible, LocallyTraversable<E> {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;
    private Traversal<E, E> localTraversal = null;

    public PropertiesStep(final Traversal traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.returnType = propertyType;
        this.propertyKeys = propertyKeys;
        this.setFunction(traverser -> {
            final Iterator<E> iterator = StreamFactory.stream(traverser.get().iterators().propertyIterator(propertyKeys))
                    .filter(p -> (propertyType.forHiddens() && p.isHidden()) || (!propertyType.forHiddens() && !p.isHidden()))
                    .map(p -> (E) (propertyType.forValues() ? p.value() : p)).iterator();
            if (null == this.localTraversal)
                return iterator;
            else {
                this.localTraversal.reset();
                TraversalHelper.getStart(this.localTraversal).addPlainStarts(iterator, traverser.bulk());
                return (Iterator) this.localTraversal;
            }

        });
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return this.propertyKeys;
    }

    @Override
    public void setLocalTraversal(final Traversal<E, E> localTraversal) {
        this.localTraversal = localTraversal;
    }

    @Override
    public Traversal<E, E> getLocalTraversal() {
        return this.localTraversal;
    }

    @Override
    public PropertiesStep<E> clone() throws CloneNotSupportedException {
        final PropertiesStep<E> clone = (PropertiesStep<E>) super.clone();
        if (null != this.localTraversal) clone.localTraversal = this.localTraversal.clone();
        return clone;
    }

    @Override
    public void reverse() {
        // TODO: only works if its element->property ... how do we do dynamic reversibility?
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }

    @Override
    public String toString() {
        return this.propertyKeys.length == 0 ?
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase(), this.localTraversal) :
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase(), Arrays.toString(this.propertyKeys), this.localTraversal);
    }
}
