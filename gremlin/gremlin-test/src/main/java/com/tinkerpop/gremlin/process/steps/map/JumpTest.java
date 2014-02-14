package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Ignore;
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
public abstract class JumpTest extends AbstractGremlinTest {
    public abstract Iterator<String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX();

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("Java gremlin doesn't compile.")
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        final Iterator<String> step = get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX();
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

        public Iterator<String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
            return null; // g.v(1).as("x").out().jump("x", h -> h.getLoops() < 2).value("name");
        }
    }
}
