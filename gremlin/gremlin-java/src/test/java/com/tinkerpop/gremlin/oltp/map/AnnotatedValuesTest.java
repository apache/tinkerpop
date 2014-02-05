package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValuesTest extends com.tinkerpop.gremlin.test.map.AnnotatedValuesTest {

    Graph g = TinkerFactory.createModern();

    @Test
    public void testCompliance() {
        //ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X() {
        super.g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X(GremlinJ.of(g).v(1).annotatedValues("locations").interval("startTime", 2004, 2006));
    }

    @Test
    public void g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value() {
        super.g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value(GremlinJ.of(g).V().annotatedValues("locations").has("startTime", 2005).value());
    }
}