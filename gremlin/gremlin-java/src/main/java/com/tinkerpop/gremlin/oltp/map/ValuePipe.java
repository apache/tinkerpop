package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuePipe<S, E> extends MapPipe<S, E> {

    public ValuePipe(final Pipeline pipeline) {
        super(pipeline, holder -> {
            final S s = holder.get();
            if (s instanceof AnnotatedValue)
                return ((AnnotatedValue<E>) s).getValue();
            else if (s instanceof Property)
                return ((Property<E>) s).get();
            else throw new IllegalStateException("A value can only be retrieved from a property or annotated value");
        });
    }
}
