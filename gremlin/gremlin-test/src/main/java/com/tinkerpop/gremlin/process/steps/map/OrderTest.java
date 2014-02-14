package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderTest {

    public void g_V_name_order(final Iterator<String> step) {
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
    }

    public void g_V_name_orderXabX(final Iterator<String> step) {
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(5));
        assertEquals("lop", names.get(4));
        assertEquals("marko", names.get(3));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(1));
        assertEquals("vadas", names.get(0));


    }

    public void g_V_orderXa_nameXb_nameX_name(final Iterator<String> step) {
        this.g_V_name_order(step);
    }
}
