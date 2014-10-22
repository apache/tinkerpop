package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraversalResultMapReduce;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.DefaultSideEffects;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Arrays;
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

    public static final String TRAVERSER_TRACKER = Graph.Key.hide("gremlin.traversalVertexProgram.traverserTracker");
    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalVertexProgram.traversalSupplier";

    private LambdaHolder<Supplier<Traversal>> traversalSupplier;
    private ThreadLocal<Traversal> traversal = new ThreadLocal<>();

    private boolean trackPaths = false;
    public Set<MapReduce> mapReducers = new HashSet<>();
    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(VOTE_TO_HALT));
    private Set<String> elementComputeKeys = new HashSet<String>() {{
        add(TRAVERSER_TRACKER);
        add(Traversal.SideEffects.SIDE_EFFECTS);
    }};

    private TraversalVertexProgram() {
    }

    /**
     * A helper method that yields a {@link com.tinkerpop.gremlin.process.Traversal.SideEffects} view of the distributed sideEffects within the currently processed {@link com.tinkerpop.gremlin.structure.Vertex}.
     *
     * @param localVertex the currently executing vertex
     * @return a sideEffect API to get and put sideEffect data onto the vertex
     */
    public static Traversal.SideEffects getLocalSideEffects(final Vertex localVertex) {
        return new DefaultSideEffects(localVertex);
    }

    /**
     * A helper method to yield a {@link Supplier} of {@link Traversal} from the {@link Configuration}.
     * The supplier is either a {@link Class}, {@link com.tinkerpop.gremlin.process.computer.util.ScriptEngineLambda}, or a direct Java8 lambda.
     *
     * @param configuration The configuration containing the public static TRAVERSAL_SUPPLIER key.
     * @return the traversal supplier in the configuration
     */
    public static Supplier<Traversal> getTraversalSupplier(final Configuration configuration) {
        return LambdaHolder.<Supplier<Traversal>>loadState(configuration, TraversalVertexProgram.TRAVERSAL_SUPPLIER).get();
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
        configuration.setProperty(VERTEX_PROGRAM, TraversalVertexProgram.class.getName());
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

        if (!(traversal.getSteps().get(0) instanceof GraphStep))
            throw new UnsupportedOperationException("TraversalVertexProgram currently only supports GraphStep starts on vertices or edges");

        final GraphStep startStep = (GraphStep) traversal.getSteps().get(0);   // TODO: make this generic to Traversal
        final String future = startStep.getNextStep() instanceof EmptyStep ? Traverser.Admin.DONE : startStep.getNextStep().getLabel();
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        if (startStep.returnsVertices()) {   // PROCESS VERTICES
            final Traverser.Admin<Vertex> traverser = this.trackPaths ?
                    new PathTraverser<>(startStep.getLabel(), vertex, traversal.sideEffects()) :
                    new SimpleTraverser<>(vertex, traversal.sideEffects());
            traverser.setFuture(future);
            traverser.detach();
            messenger.sendMessage(MessageType.Global.to(vertex), traverser);
            voteToHalt.set(false);
        } else if (startStep.returnsEdges()) {  // PROCESS EDGES
            vertex.iterators().edgeIterator(Direction.OUT, Integer.MAX_VALUE).forEachRemaining(edge -> {
                final Traverser.Admin<Edge> traverser = this.trackPaths ?
                        new PathTraverser<>(startStep.getLabel(), edge, traversal.sideEffects()) :
                        new SimpleTraverser<>(edge, traversal.sideEffects());
                traverser.setFuture(future);
                traverser.detach();
                messenger.sendMessage(MessageType.Global.to(vertex), traverser);
                voteToHalt.set(false);
            });
        } else {  // PROCESS ARBITRARY OBJECTS
            throw new UnsupportedOperationException("TraversalVertexProgram currently only supports vertex and edge starts");
        }

        vertex.property(TRAVERSER_TRACKER, new TraverserTracker());
        memory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Memory memory) {
        final Traversal traversal = this.getTraversal();
        traversal.sideEffects().setLocalVertex(vertex);

        if (this.trackPaths)
            memory.and(VOTE_TO_HALT, PathTraverserExecutor.execute(vertex, messenger, traversal));
        else
            memory.and(VOTE_TO_HALT, SimpleTraverserExecutor.execute(vertex, messenger, traversal));
        vertex.<TraverserTracker>value(TRAVERSER_TRACKER).completeIteration();
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
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MapReduce> getMapReducers() {
        return this.mapReducers;
    }

    @Override
    public String toString() {
        final Traversal traversal = this.getTraversal();
        traversal.strategies().apply();
        final String traversalString = traversal.toString().substring(1);
        return StringFactory.vertexProgramString(this, traversalString.substring(0, traversalString.length() - 1));
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

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        public Builder() {
            super(TraversalVertexProgram.class);
        }

        public Builder traversal(final String scriptEngine, final String traversalScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, TRAVERSAL_SUPPLIER, new String[]{scriptEngine, traversalScript});
            return this;
        }

        public Builder traversal(final String traversalScript) {
            return traversal(GREMLIN_GROOVY, traversalScript);
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