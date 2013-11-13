package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.FilterPipe;
import com.tinkerpop.gremlin.pipes.FlatMapPipe;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.MapPipe;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;
import com.tinkerpop.gremlin.pipes.util.PipeHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram {

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
            vertex.setProperty(GREMLINS, Arrays.asList(new Holder<>(Pipe.NONE, vertex)));
        } else {
            final Pipe pipe = getCurrentPipe(graphMemory);
            if (pipe instanceof FilterPipe) {
                pipe.addStarts(new SingleIterator<>(new Holder(pipe.getName(), vertex)));
                vertex.setProperty(GREMLINS, PipeHelper.hasNextIteration(pipe) ?
                        vertex.getValue(GREMLINS) :
                        Collections.emptyList());
            } else if (pipe instanceof FlatMapPipe) {
                pipe.addStarts(new HolderIterator<>(StreamFactory.stream(vertex.query().direction(Direction.BOTH).vertices())
                        .filter(v -> v.<List>getValue(GREMLINS).size() > 0).iterator()));
                /*StreamFactory.stream(vertex.query().direction(Direction.BOTH).vertices())
                        .filter(v -> v.<List>getValue(GREMLINS).size() > 0)
                        .flatMap(v -> new HolderIterator(v.<List<Holder>>getValue(GREMLINS)).)
                        .forEach(h -> pipe.addStarts(h)); */
                vertex.setProperty(GREMLINS, PipeHelper.toList(pipe));
            } else if (pipe instanceof MapPipe) {
                // TODO
            } else {
                throw new IllegalStateException("There are no other pipe types -- how did you get here?");
            }
        }
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