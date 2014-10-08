package com.tinkerpop.gremlin.process.util;

import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkListTest {

    @Test
    public void shouldHaveProperCountAndNotOutOfMemoryException() {
        final List<Boolean> list = new BulkList<>();
        final Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            list.add(random.nextBoolean());
        }
        assertEquals(10000000, list.size());
    }

    @Test
    public void shouldHaveCorrectBulkCounts() {
        final BulkList<String> list = new BulkList<>();
        list.add("marko");
        list.add("matthias");
        list.add("marko", 7);
        list.add("stephen");
        list.add("stephen");
        assertEquals(8, list.get("marko"));
        assertEquals(1, list.get("matthias"));
        assertEquals(2, list.get("stephen"));
        final Iterator<String> iterator = list.iterator();
        for (int i = 0; i < 11; i++) {
            if (i < 8)
                assertEquals("marko", iterator.next());
            else if (i < 9)
                assertEquals("matthias", iterator.next());
            else
                assertEquals("stephen", iterator.next());
        }
        for (int i = 0; i < 8; i++) {
            assertEquals("marko", list.get(i));
        }
        assertEquals("matthias", list.get(8));
        assertEquals("stephen", list.get(9));
        assertEquals("stephen", list.get(10));
        assertEquals(11, list.size());

    }
}
