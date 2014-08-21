package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraversalResultMapReduce;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgram<M extends TraversalMessage> implements VertexProgram<M> {

    // TODO: if not an adjacent traversal, use Local message types
    // TODO: a dual messaging system

    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalVertexProgram.traversalSupplier";
    public static final String TRAVERSAL_SUPPLIER_CLASS = "gremlin.traversalVertexProgram.traversalSupplierClass";
    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    public static final String TRAVERSER_TRACKER = Graph.Key.hide("gremlin.traverserTracker");

    private SSupplier<Traversal> traversalSupplier;
    private Class<SSupplier<Traversal>> traversalSupplierClass = null;
    private boolean trackPaths = false;
    public List<MapReduce> mapReducers = new ArrayList<>();

    public Map<String, KeyType> computeKeys = new HashMap<String, KeyType>() {{
        put(TRAVERSER_TRACKER, KeyType.CONSTANT);
    }};

    private TraversalVertexProgram() {
    }

    @Override
    public void loadState(final Configuration configuration) {
        try {
            if (configuration.containsKey(TRAVERSAL_SUPPLIER)) {
                this.traversalSupplier = VertexProgramHelper.deserialize(configuration, TRAVERSAL_SUPPLIER);
            } else {
                this.traversalSupplierClass = (Class) Class.forName(configuration.getProperty(TRAVERSAL_SUPPLIER_CLASS).toString());
                this.traversalSupplier = this.traversalSupplierClass.getConstructor().newInstance();
            }

            final Traversal traversal = this.traversalSupplier.get();
            traversal.strategies().apply();
            this.trackPaths = TraversalHelper.trackPaths(traversal);
            traversal.getSteps().stream().filter(step -> step instanceof MapReducer).forEach(step -> {
                final MapReduce mapReduce = ((MapReducer) step).getMapReduce();
                this.mapReducers.add(mapReduce);
                this.computeKeys.put(Graph.Key.hide(mapReduce.getMemoryKey()), KeyType.CONSTANT);
            });

            if (!(TraversalHelper.getEnd(traversal) instanceof SideEffectCapComputerStep))
                this.mapReducers.add(new TraversalResultMapReduce());

        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, TraversalVertexProgram.class.getName());
        if (this.traversalSupplierClass != null) {
            configuration.setProperty(TRAVERSAL_SUPPLIER_CLASS, this.traversalSupplierClass.getCanonicalName());
        } else {
            try {
                VertexProgramHelper.serialize(this.traversalSupplier, configuration, TRAVERSAL_SUPPLIER);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<M> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, memory);
        } else {
            executeOtherIterations(vertex, messenger, memory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final Memory memory) {
        final Traversal traversal = this.traversalSupplier.get();
        traversal.strategies().apply();
        final GraphStep startStep = (GraphStep) traversal.getSteps().get(0);   // TODO: make this generic to Traversal
        final String future = (traversal.getSteps().size() == 1) ? Traverser.NO_FUTURE : ((Step) traversal.getSteps().get(1)).getAs();
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        if (Vertex.class.isAssignableFrom(startStep.returnClass)) {
            final Traverser<Vertex> traverser = this.trackPaths ?
                    new PathTraverser<>(startStep.getAs(), vertex) :
                    new SimpleTraverser<>(vertex);
            traverser.setFuture(future);
            messenger.sendMessage(MessageType.Global.of(vertex), TraversalMessage.of(traverser));
            voteToHalt.set(false);
        } else if (Edge.class.isAssignableFrom(startStep.returnClass)) {
            vertex.outE().forEach(e -> {
                final Traverser<Edge> traverser = this.trackPaths ?
                        new PathTraverser<>(startStep.getAs(), e) :
                        new SimpleTraverser<>(e);
                traverser.setFuture(future);
                messenger.sendMessage(MessageType.Global.of(vertex), TraversalMessage.of(traverser));
                voteToHalt.set(false);
            });
        }
        if (this.trackPaths)
            vertex.property(TRAVERSER_TRACKER, new TraverserPathTracker());
        else
            vertex.property(TRAVERSER_TRACKER, new TraverserCountTracker());

        memory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, final Memory memory) {
        if (this.trackPaths) {
            memory.and(VOTE_TO_HALT, TraversalPathMessage.execute(vertex, messenger, this.traversalSupplier));
            vertex.<TraverserPathTracker>value(TRAVERSER_TRACKER).completeIteration();
        } else {
            memory.and(VOTE_TO_HALT, TraversalCounterMessage.execute(vertex, messenger, this.traversalSupplier));
            vertex.<TraverserCountTracker>value(TRAVERSER_TRACKER).completeIteration();
        }

    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public Map<String, KeyType> getElementComputeKeys() {
        return this.computeKeys;
    }

    @Override
    public List<MapReduce> getMapReducers() {
        return this.mapReducers;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        final Set<String> keys = new HashSet<>();
        keys.add(VOTE_TO_HALT);
        return keys;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + this.traversalSupplier.get().toString();
    }

    public SSupplier<Traversal> getTraversalSupplier() {
        return this.traversalSupplier;
    }

    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageTypes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    //////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Builder> {

        public Builder() {
            super(TraversalVertexProgram.class);
        }

        public Builder traversal(final SSupplier<Traversal> traversalSupplier) {
            try {
                VertexProgramHelper.serialize(traversalSupplier, this.configuration, TRAVERSAL_SUPPLIER);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return this;
        }

        public Builder traversal(final Class<SSupplier<Traversal>> traversalSupplierClass) {
            this.configuration.setProperty(TRAVERSAL_SUPPLIER_CLASS, traversalSupplierClass);
            return this;
        }

        // TODO Builder resolveElements(boolean) to be fed to ComputerResultStep
    }

}