package com.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PeerPressureVertexProgram implements VertexProgram<Serializable> {

    private MessageType.Local messageType = MessageType.Local.of(() -> new DefaultGraphTraversal().outE());

    public static final String CLUSTER = Graph.Key.hidden("gremlin.cluster");
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
    public Map<String, KeyType> getElementKeys() {
        return VertexProgram.createElementKeys(CLUSTER, KeyType.VARIABLE);
    }

    @Override
    public Set<String> getSideEffectKeys() {
        return Collections.emptySet();
    }


    @Override
    public Class<Serializable> getMessageClass() {
        return Serializable.class;
    }

    @Override
    public void setup(final SideEffects sideEffects) {
        sideEffects.set(VOTE_TO_HALT, false);
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Serializable> messenger, final SideEffects sideEffects) {
        if (sideEffects.isInitialIteration()) {
            vertex.property(CLUSTER, vertex.id());
            messenger.sendMessage(this.messageType, (Serializable) vertex.id());
            sideEffects.and(VOTE_TO_HALT, false);
        } else {
            final Map<Serializable, Long> votes = new HashMap<>();
            messenger.receiveMessages(this.messageType).forEach(message -> MapHelper.incr(votes, message, 1l));
            Serializable cluster = MapHelper.largestCount(votes);
            if (null == cluster) cluster = (Serializable) vertex.id();
            sideEffects.and(VOTE_TO_HALT, vertex.value(CLUSTER).equals(cluster));
            vertex.property(CLUSTER, cluster);
            messenger.sendMessage(this.messageType, cluster);
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

    //////////////////////////////

    public static Builder create() {
        return new Builder();
    }

    public static class Builder implements VertexProgram.Builder {

        private final Configuration configuration = new BaseConfiguration();

        private Builder() {
            this.configuration.setProperty(GraphComputer.VERTEX_PROGRAM, PeerPressureVertexProgram.class.getName());
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

        public Configuration getConfiguration() {
            return this.configuration;
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
