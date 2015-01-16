package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.Configuration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


/**
 * TraversalVertexProgram enables the evaluation of a {@link Traversal} on a {@link com.tinkerpop.gremlin.process.computer.GraphComputer}.
 * At the start of the computation, each {@link Vertex} (or {@link com.tinkerpop.gremlin.structure.Edge}) is assigned a single {@link Traverser}.
 * For each traverser that is local to the vertex, the vertex looks up its current location in the traversal and processes that step.
 * If the outputted traverser of the step references a local structure on the vertex (e.g. the vertex, an incident edge, its properties, or an arbitrary object),
 * then the vertex continues to compute the next traverser. If the traverser references another location in the graph,
 * then the traverser is sent to that location in the graph via a message. The messages of TraversalVertexProgram are traversers.
 * This continues until all traversers in the computation have halted.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgram implements VertexProgram<TraverserSet<?>> {

    public static final String HALTED_TRAVERSERS = "gremlin.traversalVertexProgram.haltedTraversers";
    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalVertexProgram.traversalSupplier";

    // TODO: if not an adjacent traversal, use Local message scope -- a dual messaging system.
    private static final Set<MessageScope> MESSAGE_SCOPES = new HashSet<>(Arrays.asList(MessageScope.Global.instance()));
    private static final Set<String> ELEMENT_COMPUTE_KEYS = new HashSet<>(Arrays.asList(HALTED_TRAVERSERS, TraversalSideEffects.SIDE_EFFECTS));
    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(VOTE_TO_HALT));

    private LambdaHolder<Supplier<Traversal>> traversalSupplier;
    private Traversal<?, ?> traversal;

    private final Set<MapReduce> mapReducers = new HashSet<>();

    private TraversalVertexProgram() {
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

    public Traversal getTraversal() {
        return this.traversal;
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversalSupplier = LambdaHolder.loadState(configuration, TRAVERSAL_SUPPLIER);
        if (null == this.traversalSupplier) {
            throw new IllegalArgumentException("The configuration does not have a traversal supplier");
        }
        this.traversal = this.traversalSupplier.get().get();
        for (final Step<?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecurssively(MapReducer.class, this.traversal)) {
            this.mapReducers.add(((MapReducer) mapReducer).getMapReduce());
        }
        if (!(TraversalHelper.getEnd(this.traversal) instanceof SideEffectCapStep))
            this.mapReducers.add(new TraverserMapReduce(TraversalHelper.getEnd(this.traversal)));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.traversalSupplier.storeState(configuration);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final Memory memory) {
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        if (memory.isInitialIteration()) {
            final TraverserSet<Object> haltedTraversers = new TraverserSet<>();
            vertex.property(HALTED_TRAVERSERS, haltedTraversers);

            if (!(this.traversal.asAdmin().getSteps().get(0) instanceof GraphStep))
                throw new UnsupportedOperationException("TraversalVertexProgram currently only supports GraphStep starts on vertices or edges");

            final GraphStep<Element> startStep = (GraphStep<Element>) this.traversal.asAdmin().getSteps().get(0);   // TODO: make this generic to Traversal
            final TraverserGenerator traverserGenerator = TraversalStrategies.GlobalCache.getStrategies(Graph.class).getTraverserGenerator(this.traversal);
            final String future = startStep.getNextStep().getId();
            boolean voteToHalt = true;
            final Iterator<? extends Element> starts = startStep.returnsVertices() ? IteratorUtils.of(vertex) : vertex.iterators().edgeIterator(Direction.OUT);
            while (starts.hasNext()) {
                final Element start = starts.next();
                if (ElementHelper.idExists(start.id(), startStep.getIds())) {
                    final Traverser.Admin<Element> traverser = traverserGenerator.generate(start, startStep, 1l);
                    traverser.setFutureId(future);
                    traverser.detach();
                    if (traverser.isHalted())
                        haltedTraversers.add((Traverser.Admin) traverser);
                    else {
                        voteToHalt = false;
                        messenger.sendMessage(MessageScope.Global.of(vertex), new TraverserSet<>(traverser));
                    }
                }
            }
            memory.and(VOTE_TO_HALT, voteToHalt);
        } else {
            memory.and(VOTE_TO_HALT, TraverserExecutor.execute(vertex, messenger, this.traversal));
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
        return ELEMENT_COMPUTE_KEYS;
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
    public Optional<MessageCombiner<TraverserSet<?>>> getMessageCombiner() {
        return (Optional) TraversalVertexProgramMessageCombiner.instance();
    }

    @Override
    public TraversalVertexProgram clone() throws CloneNotSupportedException {
        final TraversalVertexProgram clone = (TraversalVertexProgram) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }

    @Override
    public String toString() {
        final String traversalString = this.traversal.toString().substring(1);
        return StringFactory.vertexProgramString(this, traversalString.substring(0, traversalString.length() - 1));
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageScopes() {
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