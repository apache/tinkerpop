package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AnnotatedValuesTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, AnnotatedValue<String>> get_g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X();

    public abstract Traversal<Vertex, String> get_g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value();

    @Test
    @LoadGraphWith(MODERN)
    @Ignore("Need annoatatedValues for vertex as well as loader for modern graph")
    public void g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X() {
        final Iterator<AnnotatedValue<String>> step = get_g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X();
        System.out.println("Testing: " + step);
        final List<AnnotatedValue<String>> locations = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, locations.size());
        locations.forEach(av -> assertTrue(av.getValue().equals("brussels") || av.getValue().equals("santa fe")));
    }

    @Test
    @LoadGraphWith(MODERN)
    @Ignore("Need annoatatedValues for vertex as well as loader for modern graph")
    public void g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value() {
        final Iterator<String> step = get_g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value();
        System.out.println("Testing: " + step);
        final List<String> locations = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, locations.size());
        locations.forEach(location -> assertTrue(location.equals("kaiserslautern") || location.equals("santa fe")));
    }

    public static class JavaAnnotatedValuesTest extends AnnotatedValuesTest {

        public Traversal<Vertex, AnnotatedValue<String>> get_g_v1_annotatedValuesXlocationsX_intervalXstartTime_2004_2006X() {
            return g.v(1).annotatedValues("locations").interval("startTime", 2004, 2006);
        }

        public Traversal<Vertex, String> get_g_V_annotatedValuesXlocationsX_hasXstartTime_2005X_value() {
            return g.V().annotatedValues("locations").has("startTime", 2005).value();
        }
    }
}
