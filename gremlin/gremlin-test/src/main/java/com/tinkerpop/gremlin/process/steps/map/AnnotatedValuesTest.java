package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValuesTest {

    public void g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X(final Iterator<AnnotatedValue<String>> step) {
        System.out.println("Testing: " + step);
        final List<AnnotatedValue<String>> locations = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, locations.size());
        locations.forEach(av -> assertTrue(av.getValue().equals("brussels") || av.getValue().equals("santa fe")));
    }

    public void g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value(final Iterator<String> step) {
        System.out.println("Testing: " + step);
        final List<String> locations = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, locations.size());
        locations.forEach(location -> assertTrue(location.equals("kaiserslautern") || location.equals("santa fe")));
    }

}
