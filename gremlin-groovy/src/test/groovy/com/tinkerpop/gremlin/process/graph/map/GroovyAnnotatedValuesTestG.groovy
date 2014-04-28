package com.tinkerpop.gremlin.process.graph.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.AnnotatedValue
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyAnnotatedValuesTestG extends AnnotatedValuesTest {

    public Traversal<Vertex, AnnotatedValue<String>> get_g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X(final Object v1Id) {
        g.v(v1Id).annotatedValues('locations').interval('startTime', 2004, 2006)
    }

    public Traversal<Vertex, String> get_g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value() {
        g.V.annotatedValues('locations').has('startTime', 2005).value
    }
}
