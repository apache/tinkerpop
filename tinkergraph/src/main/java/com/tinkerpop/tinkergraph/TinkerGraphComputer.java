package com.tinkerpop.tinkergraph;

import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.olap.VertexProgram;
import com.tinkerpop.gremlin.process.olap.traversal.TraversalResult;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer, TraversalEngine {

    public static final String EXECUTION_TYPE = "tinkergraph.computer.execution-type";
    public static final String CLONE_GRAPH = "tinkergraph.computer.clone-graph";
    public static final String PARALLEL = "parallel";
    public static final String SERIAL = "serial";

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private Configuration configuration = new BaseConfiguration();
    private final TinkerGraph graph;
    private final TinkerMessenger messenger = new TinkerMessenger();

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return new TraversalResult<>(this.graph, () -> traversal);
    }

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    public GraphComputer program(final VertexProgram program) {
        this.vertexProgram = program;
        return this;
    }

    public GraphComputer configuration(final Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public Future<Graph> submit() {
        return CompletableFuture.<Graph>supplyAsync(() -> {
            final long time = System.currentTimeMillis();

            // clone the graph or operate directly on the existing graph
            final TinkerGraph g;
            if (this.configuration.getBoolean(CLONE_GRAPH, false)) {
                try {
                    g = TinkerGraph.open();
                    final ByteBufferOutputStream output = new ByteBufferOutputStream();
                    new KryoWriter(this.graph).writeGraph(output);
                    final KryoReader reader = new KryoReader.Builder(g).build();
                    reader.readGraph(new ByteBufferInputStream(output.getByteBuffer()));
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else {
                g = this.graph;
            }

            g.usesElementMemory = true;
            g.elementMemory = new TinkerElementMemory(this.isolation, this.vertexProgram.getComputeKeys());
            final boolean parallel;
            if (this.configuration.getString(EXECUTION_TYPE, PARALLEL).equals(PARALLEL))
                parallel = true;
            else if (this.configuration.getString(EXECUTION_TYPE, SERIAL).equals(SERIAL))
                parallel = false;
            else
                throw new IllegalArgumentException("The provided execution type is not supported: " + this.configuration.getString(EXECUTION_TYPE));

            // execute the vertex program
            this.vertexProgram.setup(g.memory());
            while (true) {
                if (parallel)
                    StreamFactory.parallelStream(g.V()).forEach(vertex -> this.vertexProgram.execute(vertex, this.messenger, g.memory()));
                else
                    StreamFactory.stream(g.V()).forEach(vertex -> this.vertexProgram.execute(vertex, this.messenger, g.memory()));

                g.<Graph.Memory.Computer.System>memory().incrIteration();
                g.elementMemory.completeIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(g.memory())) break;
            }

            // update runtime and return the newly computed graph
            g.<Graph.Memory.Computer.System>memory().setRuntime(System.currentTimeMillis() - time);
            return g;
        });
    }

}
