package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram {

    private MessageType.Global global = MessageType.Global.of("gremlin");

    private static final String GREMLINS = "gremlins";
    private final Supplier<Gremlin> gremlin;

    public GremlinVertexProgram(Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent("gremlin", this.gremlin);
    }

    public void execute(final Vertex vertex, final Messenger messenger, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            messenger.sendMessage(vertex, MessageType.Global.of("gremlin", vertex), 1);
        } else {
            final Pipe pipe = getCurrentPipe(graphMemory);
            // process vertices
            List<Holder> holders = (List) StreamFactory.stream(messenger.receiveMessages(vertex,global))
                    .map(m -> new Holder<>(pipe.getName(),vertex))
                    .collect(Collectors.toList());
            vertex.setProperty(GREMLINS, holders.size());

            pipe.addStarts(holders.iterator());
            StreamFactory.stream(pipe).forEach(h ->
                    messenger.sendMessage(vertex,
                            MessageType.Global.of("gremlin", (Vertex) ((Holder) h).get()), 1));
        }
    }

    public boolean terminate(final GraphMemory graphMemory) {
        Supplier<Gremlin> gremlin = graphMemory.get("gremlin");
        return !(graphMemory.getIteration() <= gremlin.get().getPipes().size());
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLINS, KeyType.VARIABLE);
    }

    private Pipe getCurrentPipe(final GraphMemory graphMemory) {
        final Supplier<Gremlin> gremlin = graphMemory.get("gremlin");
        return (Pipe) gremlin.get().identity().getPipes().get(graphMemory.getIteration());
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Supplier<Gremlin> gremlin;

        public Builder gremlin(final Supplier<Gremlin> gremlin) {
            this.gremlin = gremlin;
            return this;
        }

        public GremlinVertexProgram build() {
            return new GremlinVertexProgram(this.gremlin);
        }
    }
}