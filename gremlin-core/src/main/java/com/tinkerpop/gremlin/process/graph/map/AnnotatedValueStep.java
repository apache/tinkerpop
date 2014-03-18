package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValueStep<V> extends FlatMapStep<Vertex, AnnotatedValue<V>> {

    public String annotatedListKey;

    public AnnotatedValueStep(final Traversal traversal, final String propertyKey) {
        super(traversal);
        this.annotatedListKey = propertyKey;
        this.setFunction(holder -> {
            final Property<AnnotatedList> property = holder.get().<AnnotatedList>getProperty(propertyKey);
            return property.isPresent() ? property.get().annotatedValues() : Collections.emptyIterator();
        });
    }
}
