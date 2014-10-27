package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultTest {
    private final UUID id = UUID.fromString("AB23423F-ED64-486B-8976-DBFD0DB85318");
    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void shouldGetString() {
        final ResponseMessage msg = ResponseMessage.build(id).result("string").create();
        final Result result = new Result(msg);

        assertEquals("string", result.getString());
        assertEquals("string", result.get(String.class));
    }

    @Test
    public void shouldGetInt() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100).create();
        final Result result = new Result(msg);

        assertEquals(100, result.getInt());
        assertEquals(100, result.get(Integer.class).intValue());
    }

    @Test
    public void shouldGetByte() {
        final ResponseMessage msg = ResponseMessage.build(id).result((byte) 100).create();
        final Result result = new Result(msg);

        assertEquals((byte) 100, result.getByte());
        assertEquals((byte) 100, result.get(Byte.class).byteValue());
    }

    @Test
    public void shouldGetShort() {
        final ResponseMessage msg = ResponseMessage.build(id).result((short) 100).create();
        final Result result = new Result(msg);

        assertEquals((short) 100, result.getShort());
        assertEquals((short) 100, result.get(Short.class).shortValue());
    }

    @Test
    public void shouldGetLong() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100l).create();
        final Result result = new Result(msg);

        assertEquals((long) 100, result.getLong());
        assertEquals((long) 100, result.get(Long.class).longValue());
    }

    @Test
    public void shouldGetFloat() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100.001f).create();
        final Result result = new Result(msg);

        assertEquals(100.001f, result.getFloat(), 0.0001f);
        assertEquals(100.001f, result.get(Float.class).floatValue(), 0.0001f);
    }

    @Test
    public void shouldGetDouble() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100.001d).create();
        final Result result = new Result(msg);

        assertEquals(100.001d, result.getDouble(), 0.0001d);
        assertEquals(100.001d, result.get(Double.class), 0.0001d);
    }

    @Test
    public void shouldGetBoolean() {
        final ResponseMessage msg = ResponseMessage.build(id).result(true).create();
        final Result result = new Result(msg);

        assertEquals(true, result.getBoolean());
        assertEquals(true, result.get(Boolean.class));
    }

    @Test
    public void shouldGetVertex() {
        final Vertex v = g.v(1);
        final ResponseMessage msg = ResponseMessage.build(id).result(v).create();
        final Result result = new Result(msg);

        assertEquals(v, result.getVertex());
        assertEquals(v, result.get(Vertex.class));
        assertEquals(v, result.getElement());
        assertEquals(v, result.get(Element.class));
    }

    @Test
    public void shouldGetEdge() {
        final Edge e = g.e(11);
        final ResponseMessage msg = ResponseMessage.build(id).result(e).create();
        final Result result = new Result(msg);

        assertEquals(e, result.getEdge());
        assertEquals(e, result.get(Edge.class));
        assertEquals(e, result.getElement());
        assertEquals(e, result.get(Element.class));
    }
}
