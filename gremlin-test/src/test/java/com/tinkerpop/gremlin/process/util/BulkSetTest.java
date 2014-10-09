package com.tinkerpop.gremlin.process.util;

import org.junit.Test;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkSetTest {

    @Test
    public void shouldHaveProperCountAndNotOutOfMemoryException() {
        final Set<Boolean> list = new BulkSet<>();
        final Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            list.add(random.nextBoolean());
        }
        assertEquals(10000000, list.size());
    }

    @Test
    public void shouldHaveCorrectBulkCounts() {
        final BulkSet<String> set = new BulkSet<>();
        set.add("marko");
        set.add("matthias");
        set.add("marko", 7);
        set.add("stephen");
        set.add("stephen");
        assertEquals(8, set.get("marko"));
        assertEquals(1, set.get("matthias"));
        assertEquals(2, set.get("stephen"));
        final Iterator<String> iterator = set.iterator();
        for (int i = 0; i < 11; i++) {
            if (i < 8)
                assertEquals("marko", iterator.next());
            else if (i < 9)
                assertEquals("matthias", iterator.next());
            else
                assertEquals("stephen", iterator.next());
        }
        assertEquals(11, set.size());
    }
}
