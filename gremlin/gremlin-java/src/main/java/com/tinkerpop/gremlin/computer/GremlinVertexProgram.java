package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram {

    private static final String GREMLIN = "gremlin";
    private static final String RESULT = "result";
    private final List<String> steps;

    public GremlinVertexProgram(List<String> steps) {
        this.steps = steps;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent("steps", steps);
    }

    public void execute(final Vertex vertex, final GraphMemory graphMemory) {

        List<String> steps = graphMemory.get("steps");
        String step = steps.get(graphMemory.getIteration());
        if (step.equals("V")) {
            vertex.setProperty(GREMLIN, 1l);
        } else if (step.equals("out")) {
            long traversers = StreamFactory.stream(vertex.query().direction(Direction.IN)
                    .vertices())
                    .collect(Collectors.summingLong(v -> v.getValue(GREMLIN)));
            vertex.setProperty(GREMLIN, traversers);
        } else if (step.equals("count")) {
            graphMemory.increment(RESULT, vertex.getValue(GREMLIN));
        } else {
            throw new UnsupportedOperationException("The step '" + step + "' is not supported");
        }
    }

    public boolean terminate(final GraphMemory graphMemory) {
        List<String> steps = graphMemory.get("steps");
        return !(graphMemory.getIteration() < steps.size());
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLIN, KeyType.VARIABLE);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private List<String> steps;

        public Builder steps(final String... steps) {
            this.steps = Arrays.asList(steps);
            return this;
        }

        public GremlinVertexProgram build() {
            return new GremlinVertexProgram(this.steps);
        }
    }
}