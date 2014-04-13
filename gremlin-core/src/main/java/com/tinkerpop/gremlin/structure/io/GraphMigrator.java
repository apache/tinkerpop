package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * {@link GraphMigrator} takes the data in one graph and pipes it to another graph.  Uses the {@link KryoReader}
 * and {@link KryoWriter} by default.
 *
 * @author Alex Averbuch (alex.averbuch@gmail.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphMigrator {

    private static final KryoReader defaultKryoReader = KryoReader.create().build();
    private static final KryoWriter defaultKryoWriter = KryoWriter.create().build();

    /**
     * Use Kryo to pipe the data from one graph to another graph.  Uses all default settings for reader/writers.
     * Refer to {@link KryoReader} and {@link KryoWriter} for those settings.  To use features like incremental
     * loading, construct the reader/writers manually and utilize
     * {@link #migrateGraph(com.tinkerpop.gremlin.structure.Graph, com.tinkerpop.gremlin.structure.Graph, GraphReader, GraphWriter)}
     */
    public static void migrateGraph(final Graph fromGraph, final Graph toGraph) throws IOException {
        migrateGraph(fromGraph, toGraph, defaultKryoReader, defaultKryoWriter);
    }

    /**
     * Pipe the data from one graph to another graph.  It is important that the reader and writer utilize the
     * same format.
     *
     * @param fromGraph the graph to take data from
     * @param toGraph   the graph to take data to
     * @param reader reads from the graph written by the writer
     * @param writer writes the graph to be read by the reader
     * @throws java.io.IOException        thrown if there is an error in steam between the two graphs
     */
    public static void migrateGraph(final Graph fromGraph, final Graph toGraph,
                                    final GraphReader reader, final GraphWriter writer) throws IOException {
        // todo: if this is the standard way to "migrate" a graph, then rethink exception handling and such
        final PipedInputStream inPipe = new PipedInputStream() {
            // Default is 1024
            protected static final int PIPE_SIZE = 1024;
        };

        final PipedOutputStream outPipe = new PipedOutputStream(inPipe) {
            public void close() throws IOException {
                while (inPipe.available() > 0) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                super.close();
            }
        };

        new Thread(() -> {
            try {
                writer.writeGraph(outPipe, fromGraph);
                outPipe.flush();
                outPipe.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        reader.readGraph(inPipe, toGraph);
    }
}
