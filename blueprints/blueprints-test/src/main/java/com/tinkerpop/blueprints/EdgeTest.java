package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.StringFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdgeTest extends AbstractBlueprintsTest {
    @Test
    public void shouldHaveStandardStringRepresentation() {
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friends", v2);

        assertEquals(StringFactory.edgeString(e), e.toString());
    }

    @Test
    public void shouldCauseExceptionIfVertexRemovedMoreThanOnce() {
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        Edge e = v1.addEdge("knows", v2);

        assertNotNull(e);

        Object id = e.getId();
        e.remove();
        assertFalse(g.query().ids(id).edges().iterator().hasNext());

        // try second remove with no commit
        try {
            e.remove();
            fail("Edge cannot be removed twice.");
        } catch (IllegalStateException ise) {
            assertEquals(Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, id).getMessage(), ise.getMessage());
        }

        e = v1.addEdge("knows", v2);
        assertNotNull(e);

        id = e.getId();
        e.remove();

        // try second remove with a commit and then a second remove.  both should return the same exception
        tryCommit(g);

        try {
            e.remove();
            fail("Edge cannot be removed twice.");
        } catch (IllegalStateException ise) {
            assertEquals(Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, id).getMessage(), ise.getMessage());
        }
    }
}
