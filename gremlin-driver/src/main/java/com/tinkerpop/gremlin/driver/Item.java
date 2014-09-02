package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * An {@code Item} represents an result value from the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Item {
    final Object resultItem;

    public Item(final ResponseMessage response) {
        this.resultItem = response.getResult().getData();
    }

    public String getString() {
        return resultItem.toString();
    }

    public int getInt() {
        return Integer.parseInt(resultItem.toString());
    }

    public byte getByte() {
        return Byte.parseByte(resultItem.toString());
    }

    public short getShort() {
        return Short.parseShort(resultItem.toString());
    }

    public long getLong() {
        return Long.parseLong(resultItem.toString());
    }

    public float getFloat() {
        return Float.parseFloat(resultItem.toString());
    }

    public double getDouble() {
        return Double.parseDouble(resultItem.toString());
    }

    public boolean getBoolean() {
        return Boolean.parseBoolean(resultItem.toString());
    }

    public boolean isNull() {
        return null == resultItem;
    }

    public Vertex getVertex() {
        return (Vertex) resultItem;
    }

    public Edge getEdge() {
        return (Edge) resultItem;
    }

    public Element getElement() {
        return (Element) resultItem;
    }

    public <T> T get(final Class<? extends T> clazz) {
        return clazz.cast(this.resultItem);
    }

    @Override
    public String toString() {
        final String c = resultItem != null ? resultItem.getClass().getCanonicalName() : "null";
        return "Item{" +
                "resultItem=" + resultItem + " " +
                "class=" + c +
                '}';
    }
}
