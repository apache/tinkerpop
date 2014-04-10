package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.AnnotatedValueHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationValuesStep extends MapStep<AnnotatedValue, Map<String, Object>> {

    public AnnotationValuesStep(final Traversal traversal, final String... annotationKeyValues) {
        super(traversal);
        this.setFunction(holder -> AnnotatedValueHelper.annotationMap(holder.get(), annotationKeyValues));
    }
}
