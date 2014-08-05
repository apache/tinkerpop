package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PeerPressureVertexProgram implements VertexProgram<Pair<Serializable, Double>> {

    private MessageType.Local messageType = MessageType.Local.of(() -> GraphTraversal.<Vertex>of().outE());

    public static final String CLUSTER = Graph.Key.hide("gremlin.cluster");
    public static final String VOTE_STRENGTH = Graph.Key.hide("gremlin.voteStrength");

    private static final String MAX_ITERATIONS = "gremlin.peerPressureVertexProgram.maxIterations";
    private static final String INCIDENT_TRAVERSAL = "gremlin.peerPressureVertexProgram.incidentTraversal";
    private static final String VOTE_TO_HALT = "gremlin.peerPressureVertexProgram.voteToHalt";

    private int maxIterations = 30;

    public PeerPressureVertexProgram() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        try {
            if (configuration.containsKey(INCIDENT_TRAVERSAL)) {
                final SSupplier<Traversal> traversalSupplier = VertexProgramHelper.deserialize(configuration, INCIDENT_TRAVERSAL);
                VertexProgramHelper.verifyReversibility(traversalSupplier.get());
                this.messageType = MessageType.Local.of((SSupplier) traversalSupplier);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, PeerPressureVertexProgram.class.getName());
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        try {
            VertexProgramHelper.serialize(this.messageType.getIncidentTraversal(), configuration, INCIDENT_TRAVERSAL);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, KeyType> getElementComputeKeys() {
        return VertexProgram.createElementKeys(CLUSTER, KeyType.VARIABLE, VOTE_STRENGTH, KeyType.CONSTANT);
    }

    @Override
    public Set<String> getSideEffectComputeKeys() {
        return new HashSet<>(Arrays.asList(VOTE_TO_HALT));
    }

    @Override
    public Class<Pair<Serializable, Double>> getMessageClass() {
        return (Class) Pair.class;
    }

    @Override
    public void setup(final SideEffects sideEffects) {
        sideEffects.set(VOTE_TO_HALT, false);
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Pair<Serializable, Double>> messenger, final SideEffects sideEffects) {
        if (sideEffects.isInitialIteration()) {
            double voteStrength = 1.0d / Double.valueOf((Long) this.messageType.edges(vertex).count().next());
            vertex.property(CLUSTER, vertex.id());
            vertex.property(VOTE_STRENGTH, voteStrength);
            messenger.sendMessage(this.messageType, new Pair<>((Serializable) vertex.id(), voteStrength));
            sideEffects.and(VOTE_TO_HALT, false);
        } else {
            final Map<Serializable, Double> votes = new HashMap<>();
            votes.put(vertex.value(CLUSTER), vertex.<Double>value(VOTE_STRENGTH));
            messenger.receiveMessages(this.messageType).forEach(message -> MapHelper.incr(votes, message.getValue0(), message.getValue1()));
            Serializable cluster = PeerPressureVertexProgram.largestCount(votes);
            if (null == cluster) cluster = (Serializable) vertex.id();
            sideEffects.and(VOTE_TO_HALT, vertex.value(CLUSTER).equals(cluster));
            vertex.property(CLUSTER, cluster);
            messenger.sendMessage(this.messageType, new Pair<>(cluster, vertex.<Double>value(VOTE_STRENGTH)));
        }
    }

    @Override
    public boolean terminate(final SideEffects sideEffects) {
        final boolean voteToHalt = sideEffects.<Boolean>get(VOTE_TO_HALT) || sideEffects.getIteration() >= this.maxIterations;
        if (voteToHalt) {
            return true;
        } else {
            sideEffects.or(VOTE_TO_HALT, true);
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

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder {


        private Builder() {
            super(PeerPressureVertexProgram.class);
        }

        public Builder maxIterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        public Builder incidentTraversal(final SSupplier<Traversal<Vertex, Edge>> incidentTraversal) throws IOException {
            try {
                VertexProgramHelper.serialize(incidentTraversal, this.configuration, INCIDENT_TRAVERSAL);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }
    }

    ////////////////////////////

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
