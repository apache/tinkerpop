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
public class Result {
    final Object resultObject;

    public Result(final ResponseMessage response) {
        this.resultObject = response.getResult().getData();
    }

    public String getString() {
        return resultObject.toString();
    }

    public int getInt() {
        return Integer.parseInt(resultObject.toString());
    }

    public byte getByte() {
        return Byte.parseByte(resultObject.toString());
    }

    public short getShort() {
        return Short.parseShort(resultObject.toString());
    }

    public long getLong() {
        return Long.parseLong(resultObject.toString());
    }

    public float getFloat() {
        return Float.parseFloat(resultObject.toString());
    }

    public double getDouble() {
        return Double.parseDouble(resultObject.toString());
    }

    public boolean getBoolean() {
        return Boolean.parseBoolean(resultObject.toString());
    }

    public boolean isNull() {
        return null == resultObject;
    }

    public Vertex getVertex() {
        return (Vertex) resultObject;
    }

    public Edge getEdge() {
        return (Edge) resultObject;
    }

    public Element getElement() {
        return (Element) resultObject;
    }

    public <T> T get(final Class<? extends T> clazz) {
        return clazz.cast(this.resultObject);
    }

    public Object getObject() {
        return this.resultObject;
    }

    @Override
    public String toString() {
        final String c = resultObject != null ? resultObject.getClass().getCanonicalName() : "null";
        return "result{" +
                "object=" + resultObject + " " +
                "class=" + c +
                '}';
    }
}
