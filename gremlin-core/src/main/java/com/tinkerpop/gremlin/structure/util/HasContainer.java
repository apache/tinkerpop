package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasContainer {

    public String key;
    public BiPredicate predicate;
    public Object value;

    public HasContainer(final String key, final BiPredicate predicate, final Object value) {
        this.key = key;
        this.predicate = predicate;
        this.value = value;
        if (null == this.value && !(this.predicate instanceof Contains)) {
            throw new IllegalArgumentException("For determining the existence of a property, use the Contains predicate");
        }
    }

    public HasContainer(final String key, final Contains contains) {
        this(key, contains, null);
    }

    public HasContainer(final T accessor, final BiPredicate predicate, final Object value) {
        this(accessor.getAccessor(), predicate, value);
    }

    public HasContainer(final T accessor, final Contains contains) {
        this(accessor.getAccessor(), contains, null);
    }

    public boolean test(final Element element) {
        if (null != this.value) {

            if (this.key.equals(T.id.getAccessor()))
                return this.predicate.test(element.id(), this.value);
            else if (this.key.equals(T.label.getAccessor()))
                return this.predicate.test(element.label(), this.value);
            else if (element instanceof VertexProperty && this.key.equals(T.value.getAccessor()))
                return this.predicate.test(((VertexProperty) element).value(), this.value);
            else if (element instanceof VertexProperty && this.key.equals(T.key.getAccessor()))
                return this.predicate.test(((VertexProperty) element).key(), this.value);
            else {
                if (element instanceof Vertex) {
                    final Iterator<? extends Property> itty = element.iterators().propertyIterator(this.key);
                    while (itty.hasNext()) {
                        if (this.predicate.test(itty.next().value(), this.value))
                            return true;
                    }
                    return false;
                } else {
                    final Property property = element.property(this.key);
                    return property.isPresent() && this.predicate.test(property.value(), this.value);
                }
            }
        } else {
            return Contains.within.equals(this.predicate) ?
                    element.property(this.key).isPresent() :
                    !element.property(this.key).isPresent();
        }
    }

    public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
        if (hasContainers.size() == 0)
            return true;
        else {
            for (final HasContainer hasContainer : hasContainers) {
                if (!hasContainer.test(element))
                    return false;
            }
            return true;
        }
    }

    // note that if the user is looking for a label property key (e.g.), then it will look the same as looking for the label of the element.
    public String toString() {
        return this.value == null ?
                (this.predicate == Contains.within ?
                        "[" + Graph.System.unSystem(this.key) + "]" :
                        "[!" + Graph.System.unSystem(this.key) + "]") :
                "[" + Graph.System.unSystem(this.key) + "," + this.predicate + "," + this.value + "]";
    }
}