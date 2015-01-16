package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.util.ImmutablePath;
import com.tinkerpop.gremlin.process.util.MutablePath;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends AbstractGremlinProcessTest {

    @Test
    public void shouldHaveStandardSemanticsImplementedCorrectly() {
        Arrays.asList(MutablePath.make(), ImmutablePath.make()).forEach(path -> {
            assertTrue(path.isSimple());
            assertEquals(0, path.size());
            path = path.extend(1, "a");
            path = path.extend(2, "b");
            path = path.extend(3, "c");
            assertEquals(3, path.size());
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            path.addLabel("d");
            assertEquals(3, path.size());
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            assertEquals(Integer.valueOf(3), path.get("d"));
            assertTrue(path.hasLabel("a"));
            assertTrue(path.hasLabel("b"));
            assertTrue(path.hasLabel("c"));
            assertTrue(path.hasLabel("d"));
            assertFalse(path.hasLabel("e"));
            assertTrue(path.isSimple());
            path = path.extend(3, "e");
            assertFalse(path.isSimple());
            assertTrue(path.hasLabel("e"));
            assertEquals(4, path.size());
            assertEquals(Integer.valueOf(1), path.get(0));
            assertEquals(Integer.valueOf(2), path.get(1));
            assertEquals(Integer.valueOf(3), path.get(2));
            assertEquals(Integer.valueOf(3), path.get(3));
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHandleMultiLabelPaths() {
        Arrays.asList(MutablePath.make(), ImmutablePath.make()).forEach(path -> {
            path = path.extend("marko", "a");
            path = path.extend("stephen", "b");
            path = path.extend("matthias", "a");
            assertEquals(3, path.size());
            assertEquals(3, path.objects().size());
            assertEquals(3, path.labels().size());
            assertEquals(2, new HashSet<>(path.labels()).size());
            assertTrue(path.get("a") instanceof List);
            assertTrue(path.get("b") instanceof String);
            assertEquals(2, path.<List<String>>get("a").size());
            assertTrue(path.<List<String>>get("a").contains("marko"));
            assertTrue(path.<List<String>>get("a").contains("matthias"));
        });

        final Path path = g.V().as("x").repeat(__.out().as("y")).times(2).path().by("name").next();
        assertEquals(3, path.size());
        assertEquals(3, path.labels().size());
        assertEquals(2, new HashSet<>(path.labels()).size());
        assertTrue(path.get("x") instanceof String);
        assertTrue(path.get("y") instanceof List);
        assertEquals(2, path.<List<String>>get("y").size());
        assertTrue(path.<List<String>>get("y").contains("josh"));
        assertTrue(path.<List<String>>get("y").contains("ripple") || path.<List<String>>get("y").contains("lop"));
    }

    @Test
    public void shouldExcludeUnlabeledLabelsFromPath() {
        Arrays.asList(MutablePath.make(), ImmutablePath.make()).forEach(path -> {
            path = path.extend("marko", "a");
            path = path.extend("stephen", "b");
            path = path.extend("matthias", "c", "d");
            assertEquals(3, path.size());
            assertEquals(3, path.objects().size());
            assertEquals(3, path.labels().size());
            assertEquals(1, path.labels().get(0).size());
            assertEquals(1, path.labels().get(1).size());
            assertEquals("b", path.labels().get(1).iterator().next());
            assertEquals(2, path.labels().get(2).size());
            assertTrue(path.labels().get(2).contains("c"));
            assertTrue(path.labels().get(2).contains("d"));
        });
    }
}