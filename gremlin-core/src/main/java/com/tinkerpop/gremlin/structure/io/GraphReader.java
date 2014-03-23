package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.QuintFunction;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.io.IOException;
import java.io.InputStream;

/**
 * Functions for reading a graph and its graph elements from a different format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphReader {

    /**
     * Reads an entire graph from an {@link InputStream}.
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     */
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException;

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties but not edges.
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param vertexMaker a function to create a vertex where the first argument is the vertex identifer, the
     *                    second argument is vertex label and the last is the list of properties for it
     */
    public Vertex readVertex(final InputStream inputStream, final TriFunction<Object, String, Object[], Vertex> vertexMaker) throws IOException;

    // todo: should we be consistent with IllegalStateException on readVertex when a Direction is requested that isn't present?

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties as well as edges
     * given the direction supplied as an argument.
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param direction   the direction of edges to read.
     * @param vertexMaker a function to create a vertex where the first argument is the vertex identifer, the
     *                    second argument is vertex label and the last is the list of properties for it
     * @param edgeMaker   a function that creates an edge from the stream where the first argument is the edge
     *                    identifier, the second argument is the out vertex id, the third is the in vertex id,
     *                    the fourth is the label, and the fifth is the list of properties as key/value pairs.
     */
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final TriFunction<Object, String, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException;

    /**
     * Reads a single edge from an {@link InputStream}.
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param edgeMaker   a function that creates an edge from the stream where the first argument is the edge
     *                    identifier, the second argument is the out vertex id, the third is the in vertex id,
     *                    the fourth is the label, and the fifth is the list of properties as key/value pairs.
     */
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException;

}
