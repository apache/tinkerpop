package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.marker.CountTraversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PeerPressureVertexProgram implements VertexProgram<Pair<Serializable, Double>> {

    private MessageType.Local<?, ?> messageType = MessageType.Local.of(() -> GraphTraversal.<Vertex>of().outE());

    public static final String CLUSTER = Graph.Key.hide("gremlin.peerPressureVertexProgram.cluster");
    public static final String VOTE_STRENGTH = Graph.Key.hide("gremlin.peerPressureVertexProgram.voteStrength");

    private static final String MAX_ITERATIONS = "gremlin.peerPressureVertexProgram.maxIterations";
    private static final String DISTRIBUTE_VOTE = "gremlin.peerPressureVertexProgram.distributeVote";
    private static final String INCIDENT_TRAVERSAL = "gremlin.peerPressureVertexProgram.incidentTraversal";
    private static final String VOTE_TO_HALT = "gremlin.peerPressureVertexProgram.voteToHalt";

    private int maxIterations = 30;
    private boolean distributeVote = false;

    private static final Set<String> ELEMENT_COMPUTE_KEYS = new HashSet<>(Arrays.asList(CLUSTER, VOTE_STRENGTH));
    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(VOTE_TO_HALT));

    private PeerPressureVertexProgram() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        this.distributeVote = configuration.getBoolean(DISTRIBUTE_VOTE, false);
        try {
            if (configuration.containsKey(INCIDENT_TRAVERSAL)) {
                final Supplier<Traversal> traversalSupplier = VertexProgramHelper.deserialize(configuration, INCIDENT_TRAVERSAL);
                VertexProgramHelper.verifyReversibility(traversalSupplier.get());
                this.messageType = MessageType.Local.of((Supplier) traversalSupplier);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, PeerPressureVertexProgram.class.getName());
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(DISTRIBUTE_VOTE, this.distributeVote);
        try {
            VertexProgramHelper.serialize(this.messageType.getIncidentTraversal(), configuration, INCIDENT_TRAVERSAL);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, false);
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Pair<Serializable, Double>> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            double voteStrength = this.distributeVote ? (1.0d / Double.valueOf(this.messageType.<CountTraversal<Vertex, Edge>>edges(vertex).count().next())) : 1.0d;
            vertex.singleProperty(CLUSTER, vertex.id());
            vertex.singleProperty(VOTE_STRENGTH, voteStrength);
            messenger.sendMessage(this.messageType, new Pair<>((Serializable) vertex.id(), voteStrength));
            memory.and(VOTE_TO_HALT, false);
        } else {
            final Map<Serializable, Double> votes = new HashMap<>();
            votes.put(vertex.value(CLUSTER), vertex.<Double>value(VOTE_STRENGTH));
            messenger.receiveMessages(this.messageType).forEach(message -> MapHelper.incr(votes, message.getValue0(), message.getValue1()));
            Serializable cluster = PeerPressureVertexProgram.largestCount(votes);
            if (null == cluster) cluster = (Serializable) vertex.id();
            memory.and(VOTE_TO_HALT, vertex.value(CLUSTER).equals(cluster));
            vertex.singleProperty(CLUSTER, cluster);
            messenger.sendMessage(this.messageType, new Pair<>(cluster, vertex.<Double>value(VOTE_STRENGTH)));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || memory.getIteration() >= this.maxIterations;
        if (voteToHalt) {
            return true;
        } else {
            memory.or(VOTE_TO_HALT, true);
            return false;
        }
    }

    private static <T> T largestCount(final Map<T, Double> map) {
        T largestKey = null;
        double largestValue = Double.MIN_VALUE;
        for (Map.Entry<T, Double> entry : map.entrySet()) {
            if (entry.getValue() == largestValue) {
                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
                    largestKey = entry.getKey();
                    largestValue = entry.getValue();
                }
            } else if (entry.getValue() > largestValue) {
                largestKey = entry.getKey();
                largestValue = entry.getValue();
            }
        }
        return largestKey;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "distributeVote=" + this.distributeVote + ",maxIterations=" + this.maxIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(PeerPressureVertexProgram.class);
        }

        public Builder maxIterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        public Builder distributeVote(final boolean distributeVote) {
            this.configuration.setProperty(DISTRIBUTE_VOTE, distributeVote);
            return this;
        }

        public Builder incidentTraversal(final Supplier<Traversal<Vertex, Edge>> incidentTraversal) throws IOException {
            try {
                VertexProgramHelper.serialize(incidentTraversal, this.configuration, INCIDENT_TRAVERSAL);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }
    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageTypes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }
}
