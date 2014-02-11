package com.tinkerpop.gremlin.process.oltp.map;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        List<String> names = new ArrayList<String>();
        while (pipe.hasNext()) {
            names.add(pipe.next());
        }
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }
}
