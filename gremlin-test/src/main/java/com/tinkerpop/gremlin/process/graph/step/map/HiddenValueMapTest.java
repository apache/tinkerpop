package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HiddenValueMapTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap();

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXnameX();

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hiddenValueMap() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_hiddenValueMap();
        printTraversalForm(traversal);
        int visibleTrue = 0;
        int visibleFalse = 0;
        while (traversal.hasNext()) {
            final Map<String, List> hiddenValues = traversal.next();
            // System.out.println(hiddenValues + "!!!!!");
            // assertEquals(1, hiddenValues.size());  TODO
            assertEquals(1, hiddenValues.get("visible").size());
            if ((Boolean) hiddenValues.get("visible").get(0))
                visibleTrue++;
            else
                visibleFalse++;
        }
        assertEquals(6, visibleFalse + visibleTrue);
        assertEquals(2, visibleFalse);
        assertEquals(4, visibleTrue);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hiddenValuesXageX() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_hiddenValueMapXnameX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> hiddenValues = traversal.next();
            assertEquals(0, hiddenValues.size());
        }
        assertEquals(6, counter);
    }

    public static class StandardTest extends HiddenValueMapTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap() {
            return g.V().hiddenValueMap();
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXnameX() {
            return g.V().hiddenValueMap("name");
        }
    }
}