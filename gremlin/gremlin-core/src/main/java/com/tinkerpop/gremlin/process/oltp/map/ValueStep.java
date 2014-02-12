package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValueStep<S, E> extends MapStep<S, E> {

    public ValueStep(final Traversal traversal) {
        super(traversal, holder -> {
            final S s = holder.get();
            if (s instanceof AnnotatedValue)
                return ((AnnotatedValue<E>) s).getValue();
            else if (s instanceof Property)
                return ((Property<E>) s).get();
            else throw new IllegalStateException("A value can only be retrieved from a property or annotated value");
        });
    }
}
