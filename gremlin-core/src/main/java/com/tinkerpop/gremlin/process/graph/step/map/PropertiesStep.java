package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.PropertyType;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertiesStep<E> extends FlatMapStep<Element, E> implements Reversible {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;
    private final ChainIterator chainIterator = new ChainIterator();


    public PropertiesStep(final Traversal traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.returnType = propertyType;
        this.propertyKeys = propertyKeys;
        this.setFunction(traverser -> this.chainIterator.set(traverser.get().iterators().propertyIterator(this.propertyKeys)));
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return this.propertyKeys;
    }

    @Override
    public void reverse() {
        // TODO: only works if its element->property ... how do we do dynamic reversibility?
        TraversalHelper.replaceStep(this, new PropertyElementStep(this.traversal), this.traversal);
    }

    @Override
    public String toString() {
        return this.propertyKeys.length == 0 ?
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase()) :
                TraversalHelper.makeStepString(this, this.returnType.name().toLowerCase(), Arrays.toString(this.propertyKeys));
    }

    private class ChainIterator implements Iterator<E> {

        private Iterator<? extends Property> propertyIterator;
        private Property nextUp = null;

        public boolean hasNext() {
            return (null != this.nextUp || advance());
        }

        public E next() {
            try {
                while (true) {
                    if (null != this.nextUp) {
                        return (E) (returnType.forValues() ? this.nextUp.value() : this.nextUp);
                    } else if (!this.advance()) {
                        throw FastNoSuchElementException.instance();
                    }
                }
            } finally {
                this.nextUp = null;
            }
        }

        private boolean advance() {
            while (this.propertyIterator.hasNext()) {
                this.nextUp = this.propertyIterator.next();
                if ((!this.nextUp.isHidden() && !returnType.forHiddens()) || (this.nextUp.isHidden() && returnType.forHiddens()))
                    return true;
            }
            this.nextUp = null;
            return false;
        }

        public Iterator<E> set(final Iterator<? extends Property> propertyIterator) {
            this.propertyIterator = propertyIterator;
            return this;
        }

    }

}
