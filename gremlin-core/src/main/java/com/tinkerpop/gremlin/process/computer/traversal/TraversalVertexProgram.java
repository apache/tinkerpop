package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.strategy.HolderTraversalStrategy;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgram<M extends TraversalMessage> implements VertexProgram<M> {

    private MessageType.Global global = MessageType.Global.of();

    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalSupplier";
    public static final String TRAVERSAL_SUPPLIER_CLASS = "gremlin.traversalSupplierClass";

    private static final String TRACK_PATHS = "gremlin.traversalVertexProgram.trackPaths";
    private static final String VOTE_TO_HALT = "voteToHalt";
    public static final String TRAVERSAL_TRACKER = "gremlin.traversalVertexProgram.traversalTracker";

    private SSupplier<Traversal> traversalSupplier;
    private boolean trackPaths = false;


    public TraversalVertexProgram() {
    }

    public void initialize(final Configuration configuration) {
        try {
            this.trackPaths = configuration.getBoolean(TRACK_PATHS, false);
            if (configuration.containsKey(TRAVERSAL_SUPPLIER_CLASS)) {
                final Class<SSupplier<Traversal>> traversalSupplierClass =
                        (Class) Class.forName(configuration.getProperty(TRAVERSAL_SUPPLIER_CLASS).toString());
                this.traversalSupplier = traversalSupplierClass.getConstructor().newInstance();
            } else {
                final List byteList = configuration.getList(TRAVERSAL_SUPPLIER);
                byte[] bytes = new byte[byteList.size()];
                for (int i = 0; i < byteList.size(); i++) {
                    bytes[i] = Byte.valueOf(byteList.get(i).toString().replace("[", "").replace("]", ""));
                }
                this.traversalSupplier = (SSupplier<Traversal>) Serializer.deserializeObject(bytes);
            }
            this.trackPaths = HolderTraversalStrategy.trackPaths(this.traversalSupplier.get());
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void setup(final GraphComputer.SideEffects sideEffects) {
        sideEffects.setIfAbsent(VOTE_TO_HALT, true);
    }

    public void execute(final Vertex vertex, final Messenger<M> messenger, GraphComputer.SideEffects sideEffects) {
        if (sideEffects.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, sideEffects);
        } else {
            executeOtherIterations(vertex, messenger, sideEffects);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final GraphComputer.SideEffects sideEffects) {
        final Traversal traversal = this.traversalSupplier.get();
        traversal.optimizers().applyFinalOptimizers(traversal);
        final GraphStep startStep = (GraphStep) traversal.getSteps().get(0);   // TODO: make this generic to Traversal
        startStep.clear();
        final String future = (traversal.getSteps().size() == 1) ? Traverser.NO_FUTURE : ((Step) traversal.getSteps().get(1)).getAs();
        // TODO: Was doing some HasContainer.testAll() stuff prior to the big change (necessary?)
        // TODO: Make this an optimizer.
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        if (Vertex.class.isAssignableFrom(startStep.returnClass)) {
            final Traverser<Vertex> traverser = this.trackPaths ?
                    new PathTraverser<>(startStep.getAs(), vertex) :
                    new SimpleTraverser<>(vertex);
            traverser.setFuture(future);
            messenger.sendMessage(vertex, MessageType.Global.of(vertex), TraversalMessage.of(traverser));
            voteToHalt.set(false);
        } else if (Edge.class.isAssignableFrom(startStep.returnClass)) {
            vertex.outE().forEach(e -> {
                final Traverser<Edge> traverser = this.trackPaths ?
                        new PathTraverser<>(startStep.getAs(), e) :
                        new SimpleTraverser<>(e);
                traverser.setFuture(future);
                messenger.sendMessage(vertex, MessageType.Global.of(vertex), TraversalMessage.of(traverser));
                voteToHalt.set(false);
            });
        }
        sideEffects.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, GraphComputer.SideEffects sideEffects) {
        final Traversal traversal = this.traversalSupplier.get();
        traversal.optimizers().applyFinalOptimizers(traversal);
        ((GraphStep) traversal.getSteps().get(0)).clear();
        if (this.trackPaths) {
            final TraversalPaths tracker = new TraversalPaths(vertex);
            sideEffects.and(VOTE_TO_HALT, TraversalPathMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, traversal));
            vertex.property(TRAVERSAL_TRACKER, tracker);
        } else {
            final TraversalCounters tracker = new TraversalCounters(vertex);
            sideEffects.and(VOTE_TO_HALT, TraversalCounterMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, traversal));
            vertex.property(TRAVERSAL_TRACKER, tracker);
        }
    }

    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final GraphComputer.SideEffects sideEffects) {
        final boolean voteToHalt = sideEffects.get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            sideEffects.or(VOTE_TO_HALT, true);
            return false;
        }
    }

    public Class<M> getMessageClass() {
        return (Class) (this.trackPaths ? TraversalPathMessage.class : TraversalCounterMessage.class);
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(TRAVERSAL_TRACKER, KeyType.VARIABLE);
    }

    public String toString() {
        return "TraversalVertexProgram" + this.traversalSupplier.get().toString();
    }

    //////////////

    public static Builder create() {
        return new Builder();
    }

    public static class Builder implements VertexProgram.Builder {
        private final Configuration configuration = new BaseConfiguration();

        public Builder() {
            this.configuration.setProperty(GraphComputer.VERTEX_PROGRAM, TraversalVertexProgram.class.getName());
        }

        public Builder traversal(final SSupplier<Traversal> traversalSupplier) {
            try {
                final List<Byte> byteList = new ArrayList<>();
                final byte[] bytes = Serializer.serializeObject(traversalSupplier);
                for (byte b : bytes) {
                    byteList.add(b);
                }
                this.configuration.setProperty(TRAVERSAL_SUPPLIER, byteList);
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder traversal(final Class<SSupplier<Traversal>> traversalSupplierClass) {
            this.configuration.setProperty(TRAVERSAL_SUPPLIER_CLASS, traversalSupplierClass);
            return this;
        }

        public Configuration getConfiguration() {
            return this.configuration;
        }
    }
}