package com.tinkerpop.gremlin.process.computer.bulk;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SaveGraphVertexProgram implements VertexProgram<Object> {

    private MessageScope.Local<Object> messagesScope = MessageScope.Local.of(new BothETraversalSupplier());
    public static final String VERTEX_SERIALIZATION = Graph.Key.hide("gremlin.saveGraphVertexProgram.vertexSerialization");
    private GraphWriter writer = GraphSONWriter.build().create();

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Object> messenger, final Memory memory) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            this.writer.writeVertex(outputStream, vertex, Direction.BOTH);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        vertex.singleProperty(VERTEX_SERIALIZATION, outputStream.toString());
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.isInitialIteration();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return new HashSet<>(Arrays.asList(this.messagesScope));
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return new HashSet<>(Arrays.asList(VERTEX_SERIALIZATION));
    }

    public Set<MapReduce> getMapReducers() {
        return new HashSet<>(Arrays.asList(new SaveGraphMapReduce()));
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(SaveGraphVertexProgram.class);
        }
    }

    ////////////////////////////

    public static class BothETraversalSupplier implements Supplier<Traversal<Vertex, Edge>> {
        public Traversal<Vertex, Edge> get() {
            return GraphTraversal.<Vertex>of().bothE();
        }
    }
}
