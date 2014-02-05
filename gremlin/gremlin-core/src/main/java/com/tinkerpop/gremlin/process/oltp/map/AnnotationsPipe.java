package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.AnnotatedValueHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationsPipe extends MapPipe<AnnotatedValue, Map<String, Object>> {

    public AnnotationsPipe(final Traversal pipeline, final String... annotationKeyValues) {
        super(pipeline, holder -> AnnotatedValueHelper.annotationMap(holder.get(), annotationKeyValues));
    }
}
