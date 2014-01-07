package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.HolderIterator;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AbstractPipeTest {

    @Ignore
    public void testExpansion() {
        Pipe filter = new FilterPipe<Object>(null, s -> true);
        assertFalse(filter.hasNext());
        filter.addStarts(new HolderIterator(Arrays.asList(1, 2, 3).iterator()));
        assertTrue(filter.hasNext());
        int counter = 0;
        while (filter.hasNext()) {
            filter.next();
            counter++;
        }
        assertEquals(3, counter);
        assertFalse(filter.hasNext());

        filter.addStarts(new HolderIterator(Arrays.asList(1, 2, 3).iterator()));
        assertTrue(filter.hasNext());
        while (filter.hasNext()) {
            filter.next();
            counter++;
        }
        assertEquals(6, counter);
        assertFalse(filter.hasNext());


    }

    @Ignore
    public void testExpansion2() {
        Pipe filter1 = new FilterPipe<Object>(null, s -> true);
        Pipe filter2 = new FilterPipe<Object>(null, s -> true);
        filter2.addStarts(filter1);

        assertFalse(filter2.hasNext());
        filter1.addStarts(new HolderIterator(Arrays.asList(1, 2, 3).iterator()));
        assertTrue(filter2.hasNext());
        int counter = 0;
        while (filter2.hasNext()) {
            filter2.next();
            counter++;
        }
        assertEquals(3, counter);
        assertFalse(filter2.hasNext());

    }
}
