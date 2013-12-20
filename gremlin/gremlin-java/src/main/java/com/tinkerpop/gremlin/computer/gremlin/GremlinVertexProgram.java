package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram<GremlinMessage> {

    private MessageType.Global global = MessageType.Global.of("gremlin");

    private static final String GREMLINS = "gremlins";
    private final Supplier<Gremlin> gremlin;

    public GremlinVertexProgram(Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent("gremlin", this.gremlin);
    }

    public void execute(final Vertex vertex, final Messenger<GremlinMessage> messenger, final GraphMemory graphMemory) {
        clearGremlin(vertex);
        if (graphMemory.isInitialIteration()) {
            messenger.sendMessage(vertex, MessageType.Global.of("gremlin", vertex), GremlinMessage.of(GremlinMessage.Destination.VERTEX, vertex.getId()));
        } else {
            final Pipe pipe = getCurrentPipe(graphMemory);
            messenger.receiveMessages(vertex, global).forEach(m -> {
                if (m.destination.equals(GremlinMessage.Destination.VERTEX)) {
                    incrGremlins(vertex);
                    pipe.addStarts(new SingleIterator<>(new Holder<>(pipe.getName(), vertex)));
                    pipe.forEachRemaining(h -> {
                        Element element = ((Holder<Element>) h).get();
                        messenger.sendMessage(
                                GremlinMessage.getVertex(element),
                                MessageType.Global.of("gremlin", GremlinMessage.getVertex(element)),
                                GremlinMessage.of(element));
                    });
                } else if (m.destination.equals(GremlinMessage.Destination.EDGE)) {
                    pipe.addStarts(getEdges(vertex, m.elementId));
                    pipe.forEachRemaining(h -> {
                        Element element = ((Holder<Element>) h).get();
                        messenger.sendMessage(
                                GremlinMessage.getVertex(element),
                                MessageType.Global.of("gremlin", GremlinMessage.getVertex(element)),
                                GremlinMessage.of(element));
                    });

                } else {
                    throw new UnsupportedOperationException("No property support yet");
                }
            });


        }
    }

    private void incrGremlins(final Element element) {
        long counts = element.<Long>getAnnotation(GREMLINS).orElse(0l);
        element.setAnnotation(GREMLINS, ++counts);
    }

    private void clearGremlin(final Vertex vertex) {
        vertex.setAnnotation(GREMLINS, 0l);
        vertex.query().direction(Direction.BOTH).edges().forEach(e -> e.setAnnotation(GREMLINS, 0l));
    }

    private Iterator<Holder<Edge>> getEdges(final Vertex vertex, final Object edgeId) {
        return StreamFactory.stream(vertex.query().direction(Direction.BOTH).edges())
                .filter(e -> e.getId().equals(edgeId))
                .filter(e -> {
                    incrGremlins(e);
                    return true;
                })
                .map(e -> new Holder<>(Pipe.NONE, e))
                .iterator();
    }

    public boolean terminate(final GraphMemory graphMemory) {
        Supplier<Gremlin> gremlin = graphMemory.get("gremlin");
        return !(graphMemory.getIteration() < gremlin.get().getPipes().size());
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLINS, KeyType.VARIABLE);
    }

    private Pipe getCurrentPipe(final GraphMemory graphMemory) {
        final Supplier<Gremlin> gremlin = graphMemory.get("gremlin");
        return (Pipe) gremlin.get().getPipes().get(graphMemory.getIteration());
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