package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValuePipe<E> extends MapPipe<Property<E>, E> {

    public PropertyValuePipe(final Pipeline pipeline) {
        super(pipeline, holder -> holder.get().get());
    }
}
