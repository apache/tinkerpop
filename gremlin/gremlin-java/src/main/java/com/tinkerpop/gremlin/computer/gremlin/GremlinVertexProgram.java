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
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram<GremlinMessage> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    private static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_PIPELINE = "gremlinPipeline";
    public static final String GRAPH_GREMLINS = "graphGremlins";
    public static final String OBJECT_GREMLINS = "objectGremlins";
    private final Supplier<Gremlin> gremlin;

    public GremlinVertexProgram(final Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_PIPELINE, this.gremlin);
    }

    public void execute(final Vertex vertex, final Messenger<GremlinMessage> messenger, final GraphMemory graphMemory) {

        if (graphMemory.isInitialIteration()) {
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(vertex, new Holder<>(Pipe.NONE, vertex)));
        } else {
            final Map<Object, List<Holder>> previousObjectCounters = vertex.<HashMap<Object, List<Holder>>>getProperty(OBJECT_GREMLINS).orElse(new HashMap<>());
            final Map<Object, List<Holder>> graphCounters = new HashMap<>();
            final Map<Object, List<Holder>> objectCounters = new HashMap<>();
            final List<Object> starts = new ArrayList<>();
            final Pipe pipe = getCurrentPipe(graphMemory);

            // RECEIVE MESSAGES
            messenger.receiveMessages(vertex, this.global).forEach(m -> {
                if (m.destination.equals(GremlinMessage.Destination.VERTEX)) {
                    m.getHolder().set(vertex);
                    starts.add(m.getHolder());
                    MapHelper.incr(graphCounters, vertex, m.getHolder());
                } else if (m.destination.equals(GremlinMessage.Destination.EDGE)) {
                    this.getEdge(vertex, m).ifPresent(edge -> {
                        m.getHolder().set(edge);
                        starts.add(m.getHolder());
                        MapHelper.incr(graphCounters, edge, m.getHolder());
                    });
                } else if (m.destination.equals(GremlinMessage.Destination.PROPERTY)) {
                    this.getProperty(vertex, m).ifPresent(property -> {
                        m.getHolder().set(property);
                        starts.add(m.getHolder());
                        MapHelper.incr(graphCounters, property, m.getHolder());
                    });
                } else {
                    throw new UnsupportedOperationException("The provided message can not be processed: " + m);
                }
            });
            // process local object messages
            previousObjectCounters.forEach((a, b) -> {
                b.forEach(holder -> {
                    starts.add(holder);
                });
            });


            // EXECUTE PIPELINE WITH LOCAL STARTS AND SEND MESSAGES TO ENDS
            starts.forEach(start -> {
                pipe.addStarts(new SingleIterator<>(start));
                pipe.forEachRemaining(h -> {
                    final Holder holder = (Holder) h;
                    final Object end = holder.get();
                    // System.out.println(start + "-->" + end + " [" + counters.get(start) + "]");
                    if (end instanceof Element || end instanceof Property) {
                        // TODO: (OPTIMIZATION) IF THE ELEMENT IS ADJACENT USE LOCAL MESSAGE
                        // TODO: (OPTIMIZATION) IF THE ELEMENT IS INCIDENT USE OBJECT COUNTERS MAP
                        messenger.sendMessage(
                                vertex,
                                MessageType.Global.of(GREMLIN_MESSAGE, Messenger.getHostingVertices(end)),
                                GremlinMessage.of(end, holder));
                    } else {
                        if (graphCounters.containsKey(((Holder) start).get()))
                            MapHelper.incr(objectCounters, end, holder);
                        else if (previousObjectCounters.containsKey(((Holder) start).get()))
                            MapHelper.incr(objectCounters, end, holder);
                        else
                            throw new IllegalStateException("The provided start does not have a recorded history: " + start);
                    }
                });
            });

            // UPDATE LOCAL STARTS WITH COUNTS
            if (graphCounters.size() > 0)
                vertex.setProperty(GRAPH_GREMLINS, graphCounters);
            if (objectCounters.size() > 0)
                vertex.setProperty(OBJECT_GREMLINS, objectCounters);
        }
    }

    private Optional<Edge> getEdge(final Vertex vertex, final GremlinMessage message) {
        // TODO: WHY IS THIS NOT LIKE FAUNUS WITH A BOTH?
        // TODO: I KNOW WHY -- CAUSE OF HOSTING VERTICES IS BOTH IN/OUT WHICH IS NECESSARY FOR EDGE MUTATIONS
        return StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                .filter(e -> e.getId().equals(message.elementId))
                .findFirst();
    }

    private Optional<Property> getProperty(final Vertex vertex, final GremlinMessage message) {
        if (message.elementId.equals(vertex.getId())) {
            final Property property = vertex.getProperty(message.propertyKey);
            return property.isPresent() ? Optional.of(property) : Optional.empty();
        } else {
            // TODO: WHY IS THIS NOT LIKE FAUNUS WITH A BOTH?
            // TODO: I KNOW WHY -- CAUSE OF HOSTING VERTICES IS BOTH IN/OUT WHICH IS NECESSARY FOR EDGE MUTATIONS
            return (Optional) StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                    .filter(e -> e.getId().equals(message.elementId))
                    .map(e -> e.getProperty(message.propertyKey))
                    .findFirst();
        }
    }

    private Pipe getCurrentPipe(final GraphMemory graphMemory) {
        final Supplier<Gremlin> gremlin = graphMemory.get(GREMLIN_PIPELINE);
        return (Pipe) gremlin.get().getPipes().get(graphMemory.getIteration());
    }


    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final GraphMemory graphMemory) {
        Supplier<Gremlin> gremlin = graphMemory.get(GREMLIN_PIPELINE);
        return !(graphMemory.getIteration() < gremlin.get().getPipes().size());
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(
                GRAPH_GREMLINS, KeyType.VARIABLE,
                OBJECT_GREMLINS, KeyType.VARIABLE);
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