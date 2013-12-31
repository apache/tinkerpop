package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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
import java.util.concurrent.atomic.AtomicLong;
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
        //final AtomicLong currentCounts = new AtomicLong(0l);
        if (graphMemory.isInitialIteration()) {
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(vertex, 1l));
        } else {
            final Pipe pipe = getCurrentPipe(graphMemory);
            messenger.receiveMessages(vertex, global).forEach(m -> {
                if (m.destination.equals(GremlinMessage.Destination.VERTEX)) {
                    incrGremlins(vertex, m.counts);
                    //currentCounts.incrementAndGet();
                    pipe.addStarts(new SingleIterator<>(new Holder<>(Pipe.NONE, vertex))); // TODO: use pipe name for paths
                } else if (m.destination.equals(GremlinMessage.Destination.EDGE)) {
                    pipe.addStarts(getEdge(vertex, m));
                } else if (m.destination.equals(GremlinMessage.Destination.PROPERTY)) {
                    pipe.addStarts(getProperty(vertex, m));
                } else {
                    throw new UnsupportedOperationException("This object type has not been handled yet: " + m);
                }
            });

            //vertex.setProperty(GREMLINS,currentCounts.get());
            pipe.forEachRemaining(h -> {
                final Object object = ((Holder<Object>) h).get();
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GREMLIN_MESSAGE, Messenger.getHostingVertices(object)),
                        GremlinMessage.of(object, getGremlins(vertex)));
            });
        }
    }

    private void incrGremlins(final Object object, final long count) {
        if (object instanceof Element) {
            Element element = (Element) object;
            long counts = element.<Long>getProperty(GREMLINS).orElse(0l);
            element.setProperty(GREMLINS, count + counts);
        } else {
            Property property = (Property) object;
            long counts = property.<Long>getAnnotation(GREMLINS).orElse(0l);
            property.setAnnotation(GREMLINS, count + counts);
        }
    }

    private long getGremlins(final Object object) {
        return object instanceof Element ?
                ((Element) object).<Long>getProperty(GREMLINS).orElse(0l) :
                ((Property) object).<Long>getAnnotation(GREMLINS).orElse(0l);
    }

    private Iterator<Holder<Edge>> getEdge(final Vertex vertex, final GremlinMessage message) {
        return StreamFactory.stream(vertex.query().direction(Direction.BOTH).edges())
                .filter(e -> e.getId().equals(message.elementId))
                .filter(e -> {
                    incrGremlins(e, message.counts);
                    return true;
                })
                .map(e -> new Holder<>(Pipe.NONE, e))
                .iterator();
    }

    private Iterator<Holder<Property>> getProperty(final Vertex vertex, final GremlinMessage message) {
        if (message.elementId.equals(vertex.getId())) {
            final Property property = vertex.getProperty(message.propertyKey);
            incrGremlins(property, message.counts);
            return (Iterator) Arrays.asList(new Holder<>(Pipe.NONE, property)).iterator();
        } else {
            return (Iterator) StreamFactory.stream(vertex.query().direction(Direction.BOTH).edges())
                    .filter(e -> e.getId().equals(message.elementId))
                    .map(e -> e.getProperty(message.propertyKey))
                    .filter(p -> {
                        incrGremlins(p, message.counts);
                        return true;
                    }).map(p -> new Holder<>(Pipe.NONE, p))
                    .iterator();
        }
    }

    private void clearGremlins(final Vertex vertex) {
        // TODO: WHY DO I HAVE TO DO THIS?
        // TODO: THERE SHOULD BE A WAY TO DROP THE MEMORY STRUCTURE OF A BSP-CYCLE IN ONE FELL SWOOP.
        vertex.setProperty(GREMLINS, 0l);
        vertex.getProperties().values().forEach(p -> p.setAnnotation(GREMLINS, 0l));
        vertex.query().direction(Direction.BOTH).edges().forEach(e -> {
            e.setProperty(GREMLINS, 0l);
            e.getProperties().values().forEach(p -> p.setAnnotation(GREMLINS, 0l));
        });
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