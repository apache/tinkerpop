package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.AnnotatedValueHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationsStep extends MapStep<AnnotatedValue, Map<String, Object>> {

    public AnnotationsStep(final Traversal traversal, final String... annotationKeyValues) {
        super(traversal, holder -> AnnotatedValueHelper.annotationMap(holder.get(), annotationKeyValues));
    }
}
