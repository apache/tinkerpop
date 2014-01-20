package com.tinkerpop.gremlin.olap.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.olap.GremlinResult;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuesTest extends com.tinkerpop.gremlin.test.map.ValuesTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_values() {
        super.g_V_values(new GremlinResult<>(g, () -> Gremlin.of().V().values()));
    }

    @Test
    public void g_V_valuesXname_ageX() {
        super.g_V_valuesXname_ageX(new GremlinResult<>(g, () -> Gremlin.of().V().values("name", "age")));
    }

    @Test
    public void g_E_valuesXid_label_weightX() {
        super.g_E_valuesXid_label_weightX(new GremlinResult<>(g, () -> Gremlin.of().E().values("id", "label", "weight")));
    }

    @Test
    public void g_v1_outXcreatedX_values() {
        super.g_v1_outXcreatedX_values(new GremlinResult<>(g, () -> Gremlin.of().v("1").out("created").values()));
    }
}