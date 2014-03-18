package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class JumpTest extends AbstractGremlinProcessTest {
    public abstract Iterator<String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        final Iterator<String> step = get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(convertToId("marko"));
        System.out.println("Testing: " + step);
        List<String> names = new ArrayList<>();
        while (step.hasNext()) {
            names.add(step.next());
        }
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    public static class JavaJumpTest extends JumpTest {
        public JavaJumpTest() {
            requiresGraphComputer = false;
        }

        public Iterator<String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).as("x").out().jump("x", h -> h.getLoops() < 2).value("name");
        }
    }

    public static class JavaComputerJumpTest extends JumpTest {
        public JavaComputerJumpTest() {
            requiresGraphComputer = true;
        }

        public Iterator<String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            // todo: this test does not yet pass
            return g.v(v1Id).as("x").out().jump("x", h -> h.getLoops() < 2).<String>value("name"); // .submit(g.compute());
        }
    }
}
