package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.util.AnnotatedValueHelper;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationsPipe extends MapPipe<AnnotatedValue, Map<String, Object>> {

    public AnnotationsPipe(final Pipeline pipeline, final String... annotationKeyValues) {
        super(pipeline, holder -> AnnotatedValueHelper.annotationMap(holder.get(), annotationKeyValues));
    }
}
