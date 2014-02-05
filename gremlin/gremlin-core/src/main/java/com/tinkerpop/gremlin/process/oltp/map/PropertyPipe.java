package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyPipe<E> extends MapPipe<Element, Property<E>> {

    public PropertyPipe(final Traversal pipeline, final String key) {
        super(pipeline, holder -> holder.get().getProperty(key));
    }
}
