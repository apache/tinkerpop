package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Annotatable;
import com.tinkerpop.blueprints.Blueprints;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram<GremlinMessage> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    private static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_PIPELINE = "gremlinPipeline";
    private static final String GREMLINS = "gremlins";
    private final Supplier<Gremlin> gremlin;

    public GremlinVertexProgram(Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_PIPELINE, this.gremlin);
    }

    public void execute(final Vertex vertex, final Messenger<GremlinMessage> messenger, final GraphMemory graphMemory) {
        clearGremlins(vertex);
        if (graphMemory.isInitialIteration()) {
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(vertex));
        } else {
            final Pipe pipe = getCurrentPipe(graphMemory);
            messenger.receiveMessages(vertex, global).forEach(m -> {
                if (m.destination.equals(Blueprints.VERTEX)) {
                    incrGremlins(vertex);
                    pipe.addStarts(new SingleIterator<>(new Holder<>(pipe.getName(), vertex)));
                } else if (m.destination.equals(Blueprints.EDGE)) {
                    pipe.addStarts(getEdges(vertex, m.elementId));
                } else if (m.destination.equals(Blueprints.PROPERTY)) {
                    pipe.addStarts(getProperty(vertex, m.elementId, m.propertyKey));
                } else {
                    throw new UnsupportedOperationException("This object type has not been handled yet: " + m);
                }

                pipe.forEachRemaining(h -> {
                    final Object object = ((Holder<Object>) h).get();
                    messenger.sendMessage(
                            vertex,
                            MessageType.Global.of(GREMLIN_MESSAGE, Messenger.getHostingVertex(object)),
                            GremlinMessage.of(object));
                });
            });


        }
    }

    private void incrGremlins(final Annotatable annotatable) {
        long counts = annotatable.<Long>getAnnotation(GREMLINS).orElse(0l);
        annotatable.setAnnotation(GREMLINS, ++counts);
    }

    private void clearGremlins(final Vertex vertex) {
        // TODO: WHY DO I HAVE TO DO THIS?
        // TODO: THERE SHOULD BE A WAY TO DROP THE MEMORY STRUCTURE OF A BSP-CYCLE IN ONE FELL SWOOP.
        vertex.setAnnotation(GREMLINS, null);
        vertex.getProperties().values().forEach(p -> p.setAnnotation(GREMLINS, null));
        vertex.query().direction(Direction.BOTH).edges().forEach(e -> {
            e.setAnnotation(GREMLINS, null);
            e.getProperties().values().forEach(p -> p.setAnnotation(GREMLINS, null));
        });
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

    private Iterator<Holder<Property>> getProperty(final Vertex vertex, final Object elementId, final String propertyKey) {
        if (elementId.equals(vertex.getId())) {
            final Property property = vertex.getProperty(propertyKey);
            incrGremlins(property);
            return (Iterator) Arrays.asList(new Holder<>(Pipe.NONE, property)).iterator();
        } else {
            return (Iterator) StreamFactory.stream(vertex.query().direction(Direction.BOTH).edges())
                    .filter(e -> e.getId().equals(elementId))
                    .map(e -> e.getProperty(propertyKey))
                    .filter(p -> {
                        incrGremlins(p);
                        return true;
                    }).map(p -> new Holder<>(Pipe.NONE, p))
                    .iterator();
        }
    }


    public boolean terminate(final GraphMemory graphMemory) {
        Supplier<Gremlin> gremlin = graphMemory.get(GREMLIN_PIPELINE);
        return !(graphMemory.getIteration() < gremlin.get().getPipes().size());
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLINS, KeyType.VARIABLE);
    }

    private Pipe getCurrentPipe(final GraphMemory graphMemory) {
        final Supplier<Gremlin> gremlin = graphMemory.get(GREMLIN_PIPELINE);
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