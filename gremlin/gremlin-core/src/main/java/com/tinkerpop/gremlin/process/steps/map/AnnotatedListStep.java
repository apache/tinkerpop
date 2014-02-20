package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListStep<V> extends MapStep<Vertex, AnnotatedList<V>> {

    public String annotatedListKey;

    public AnnotatedListStep(final Traversal traversal, final String annotatedListKey) {
        super(traversal);
        this.annotatedListKey = annotatedListKey;
        this.setFunction(holder -> {
            final Property<AnnotatedList> property = holder.get().<AnnotatedList>getProperty(annotatedListKey);
            //return property.isPresent() ? property.get() : (AnnotatedList) NO_OBJECT;
            return property.get();
        });
    }
}
