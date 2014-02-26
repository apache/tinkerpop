package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {

    public void writeGraph(final OutputStream outputStream) throws IOException;

    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException;

    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException; // only writes the vertex/properties, no edges

    public void writeEdge(final OutputStream outputStream, final Edge E) throws IOException;

}
