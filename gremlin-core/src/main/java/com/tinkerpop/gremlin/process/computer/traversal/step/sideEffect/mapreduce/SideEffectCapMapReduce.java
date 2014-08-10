package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapMapReduce implements MapReduce {

    public static final String SIDE_EFFECT_CAP_STEP_SIDE_EFFECT_KEY = "gremlin.sideEffectCapStep.sideEffectKey";

    private String sideEffectKey;
    private Traversal traversal;

    public SideEffectCapMapReduce() {

    }

    public SideEffectCapMapReduce(final SideEffectCapComputerStep step) {
        this.sideEffectKey = step.getSideEffectAs();
        this.traversal = step.getTraversal();
    }

    public SideEffectCapMapReduce(final SideEffectCapStep step) {
        this.sideEffectKey = step.getSideEffectAs();
        this.traversal = step.getTraversal();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(SIDE_EFFECT_CAP_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        try {
            this.sideEffectKey = configuration.getString(SIDE_EFFECT_CAP_STEP_SIDE_EFFECT_KEY);
            this.traversal = ((SSupplier<Traversal>) VertexProgramHelper.deserialize(configuration, TraversalVertexProgram.TRAVERSAL_SUPPLIER)).get();
            this.traversal.strategies().apply();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        return false;
    }

    @Override
    public Object generateSideEffect(final Iterator keyValues) {
        final Object result = ((MapReducer) ((Stream<Step>) this.traversal.getSteps().stream())
                .filter(step -> step instanceof MapReducer)
                .filter(step -> !(step instanceof SideEffectCapComputerStep))
                .filter(step -> step instanceof SideEffectCapable)
                .filter(step -> step.getAs().equals(this.sideEffectKey))
                .findFirst().get())
                .getMapReduce()
                .generateSideEffect(keyValues);
        return result;
    }

    @Override
    public void addToSideEffects(final SideEffects sideEffects, final Iterator keyValues) {

    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }
}