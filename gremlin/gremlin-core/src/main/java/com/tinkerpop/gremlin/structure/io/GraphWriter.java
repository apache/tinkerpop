package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Functions for writing a graph and its elements to a different format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {

    /**
     * Write the entire graph to a stream.
     */
    public void writeGraph(final OutputStream outputStream) throws IOException;

    /**
     * Write a vertex to a stream with its associated edges.  Only write edges as defined by the requested direction.
     *
     * @param outputStream The stream to write to.
     * @param v The vertex to write.
     * @param direction If direction is null then no edges are written.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException;

    /**
     * Write a vertex to a stream without writing its edges.
     *
     * @param outputStream The stream to write to.
     * @param v The vertex to write.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException;

    /**
     * Write an edge to a stream.
     */
    public void writeEdge(final OutputStream outputStream, final Edge E) throws IOException;

}
