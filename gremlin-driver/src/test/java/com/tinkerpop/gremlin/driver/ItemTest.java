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
public class ItemTest {
    private final UUID id = UUID.fromString("AB23423F-ED64-486B-8976-DBFD0DB85318");
    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void shouldGetString() {
        final ResponseMessage msg = ResponseMessage.build(id).result("string").create();
        final Item item = new Item(msg);

        assertEquals("string", item.getString());
        assertEquals("string", item.get(String.class));
    }

    @Test
    public void shouldGetInt() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100).create();
        final Item item = new Item(msg);

        assertEquals(100, item.getInt());
        assertEquals(100, item.get(Integer.class).intValue());
    }

    @Test
    public void shouldGetByte() {
        final ResponseMessage msg = ResponseMessage.build(id).result((byte) 100).create();
        final Item item = new Item(msg);

        assertEquals((byte) 100, item.getByte());
        assertEquals((byte) 100, item.get(Byte.class).byteValue());
    }

    @Test
    public void shouldGetShort() {
        final ResponseMessage msg = ResponseMessage.build(id).result((short) 100).create();
        final Item item = new Item(msg);

        assertEquals((short) 100, item.getShort());
        assertEquals((short) 100, item.get(Short.class).shortValue());
    }

    @Test
    public void shouldGetLong() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100l).create();
        final Item item = new Item(msg);

        assertEquals((long) 100, item.getLong());
        assertEquals((long) 100, item.get(Long.class).longValue());
    }

    @Test
    public void shouldGetFloat() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100.001f).create();
        final Item item = new Item(msg);

        assertEquals(100.001f, item.getFloat(), 0.0001f);
        assertEquals(100.001f, item.get(Float.class).floatValue(), 0.0001f);
    }

    @Test
    public void shouldGetDouble() {
        final ResponseMessage msg = ResponseMessage.build(id).result(100.001d).create();
        final Item item = new Item(msg);

        assertEquals(100.001d, item.getDouble(), 0.0001d);
        assertEquals(100.001d, item.get(Double.class), 0.0001d);
    }

    @Test
    public void shouldGetBoolean() {
        final ResponseMessage msg = ResponseMessage.build(id).result(true).create();
        final Item item = new Item(msg);

        assertEquals(true, item.getBoolean());
        assertEquals(true, item.get(Boolean.class));
    }

    @Test
    public void shouldGetVertex() {
        final Vertex v = g.v(1);
        final ResponseMessage msg = ResponseMessage.build(id).result(v).create();
        final Item item = new Item(msg);

        assertEquals(v, item.getVertex());
        assertEquals(v, item.get(Vertex.class));
        assertEquals(v, item.getElement());
        assertEquals(v, item.get(Element.class));
    }

    @Test
    public void shouldGetEdge() {
        final Edge e = g.e(11);
        final ResponseMessage msg = ResponseMessage.build(id).result(e).create();
        final Item item = new Item(msg);

        assertEquals(e, item.getEdge());
        assertEquals(e, item.get(Edge.class));
        assertEquals(e, item.getElement());
        assertEquals(e, item.get(Element.class));
    }
}
