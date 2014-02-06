package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.test.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectTest extends com.tinkerpop.gremlin.test.sideeffect.SideEffectTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_sideEffectXstore_aX_valueXnameX() {
        //   final List<Vertex> a = new ArrayList<>();
        //   super.g_v1_sideEffectXstore_aX_valueXnameX(GremlinJ.of(g).v(1).sideEffect(holder -> {
        //       a.clear();
        //       a.add(holder.get());
        //   }).value("name"));
        //   assertEquals(g.v(1).get(), a.get(0));
    }

    @Test
    public void g_v1_out_sideEffectXincr_cX_valueXnameX() {
        //   final List<Integer> c = new ArrayList<>();
        //   c.add(0);
        //   super.g_v1_out_sideEffectXincr_cX_valueXnameX(GremlinJ.of(g).v(1).out().sideEffect(holder -> {
        //       Integer temp = c.get(0);
        //       c.clear();
        //       c.add(temp + 1);
        //    }).value("name"));
        //   assertEquals(new Integer(3), c.get(0));
    }

    @Test
    public void g_v1_out_sideEffectXX_valueXnameX() {
        //  super.g_v1_out_sideEffectXX_valueXnameX(GremlinJ.of(g).v(1).out().sideEffect(holder -> {
        //   }).value("name"));
    }
}