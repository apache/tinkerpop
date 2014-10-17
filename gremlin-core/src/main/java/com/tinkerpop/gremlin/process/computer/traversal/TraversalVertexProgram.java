package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraversalResultMapReduce;
import com.tinkerpop.gremlin.process.computer.util.AbstractBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgram implements VertexProgram<Traverser.Admin<?>> {

    // TODO: if not an adjacent traversal, use Local message types
    // TODO: a dual messaging system
    // TODO: thread local for Traversal so you don't have to keep compiling it over and over again

    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    public static final String TRAVERSER_TRACKER = Graph.Key.hide("gremlin.traverserTracker");
    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalVertexProgram.traversalSupplier";

    private LambdaHolder<Supplier<Traversal>> traversalSupplier;
    private ThreadLocal<Traversal> traversal = new ThreadLocal<>();

    private boolean trackPaths = false;
    public Set<MapReduce> mapReducers = new HashSet<>();
    private Set<String> elementComputeKeys = new HashSet<String>() {{
        add(TRAVERSER_TRACKER);
        add(Traversal.SideEffects.DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
    }};

    private TraversalVertexProgram() {
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversalSupplier = LambdaHolder.loadState(configuration, TRAVERSAL_SUPPLIER);
        if (null == this.traversalSupplier) {
            throw new IllegalArgumentException("The configuration does not have a traversal supplier");
        }
        final Traversal<?, ?> traversal = this.traversalSupplier.get().get();
        this.trackPaths = TraversalHelper.trackPaths(traversal);
        traversal.getSteps().stream().filter(step -> step instanceof MapReducer).forEach(step -> {
            final MapReduce mapReduce = ((MapReducer) step).getMapReduce();
            this.mapReducers.add(mapReduce);
        });

        if (!(TraversalHelper.getEnd(traversal) instanceof SideEffectCapStep))
            this.mapReducers.add(new TraversalResultMapReduce());

    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GraphComputer.VERTEX_PROGRAM, TraversalVertexProgram.class.getName());
        this.traversalSupplier.storeState(configuration);
    }

    public Traversal getTraversal() {
        try {
            Traversal traversal = this.traversal.get();
            if (null != traversal)
                return traversal;
            else {
                traversal = this.traversalSupplier.get().get();
                this.traversal.set(traversal);
                return traversal;
            }

        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, memory);
        } else {
            executeOtherIterations(vertex, messenger, memory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Memory memory) {
        final Traversal traversal = this.getTraversal();
        traversal.sideEffects().setLocalVertex(vertex);

        final GraphStep startStep = (GraphStep) traversal.getSteps().get(0);   // TODO: make this generic to Traversal
        final String future = startStep.getNextStep() instanceof EmptyStep ? Traverser.Admin.DONE : startStep.getNextStep().getLabel();
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);               // TODO: SIDE-EFFECTS IN TRAVERSAL IN OLAP!
        if (startStep.returnsVertices()) {   // PROCESS VERTICES
            final Traverser.Admin<Vertex> traverser = this.trackPaths ?
                    new PathTraverser<>(startStep.getLabel(), vertex, null) :
                    new SimpleTraverser<>(vertex, null);
            traverser.setFuture(future);
            traverser.deflate();
            messenger.sendMessage(MessageType.Global.to(vertex), traverser);
            voteToHalt.set(false);
        } else if (startStep.returnsEdges()) {  // PROCESS EDGES
            vertex.iterators().edgeIterator(Direction.OUT, Integer.MAX_VALUE).forEachRemaining(edge -> {
                final Traverser.Admin<Edge> traverser = this.trackPaths ?
                        new PathTraverser<>(startStep.getLabel(), edge, null) :
                        new SimpleTraverser<>(edge, null);
                traverser.setFuture(future);
                traverser.deflate();
                messenger.sendMessage(MessageType.Global.to(vertex), traverser);
                voteToHalt.set(false);
            });
        } else {  // PROCESS ARBITRARY OBJECTS
            throw new UnsupportedOperationException("TraversalVertexProgram currently only supports vertex and edge starts");
        }

        vertex.property(TRAVERSER_TRACKER, this.trackPaths ? new TraverserPathTracker() : new TraverserCountTracker());
        memory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Memory memory) {
        final Traversal traversal = this.getTraversal();
        traversal.sideEffects().setLocalVertex(vertex);

        if (this.trackPaths) {
            memory.and(VOTE_TO_HALT, PathTraverserExecutor.execute(vertex, messenger, traversal));
            vertex.<TraverserPathTracker>value(TRAVERSER_TRACKER).completeIteration();
        } else {
            memory.and(VOTE_TO_HALT, SimpleTraverserExecutor.execute(vertex, messenger, traversal));
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
    public Set<String> getElementComputeKeys() {
        return this.elementComputeKeys;
    }

    @Override
    public Set<MapReduce> getMapReducers() {
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
        final Traversal traversal = this.getTraversal();
        traversal.strategies().apply();
        return StringFactory.vertexProgramString(this, traversal.toString());
    }

    @Override
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

        public Builder traversal(final String scriptEngine, final String traversalScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, TRAVERSAL_SUPPLIER, new String[]{scriptEngine, traversalScript});
            return this;
        }

        public Builder traversal(final Supplier<Traversal> traversal) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, TRAVERSAL_SUPPLIER, traversal);
            return this;
        }

        public Builder traversal(final Class<Supplier<Traversal>> traversalClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, TRAVERSAL_SUPPLIER, traversalClass);
            return this;
        }

        // TODO Builder resolveElements(boolean) to be fed to ComputerResultStep
    }

}