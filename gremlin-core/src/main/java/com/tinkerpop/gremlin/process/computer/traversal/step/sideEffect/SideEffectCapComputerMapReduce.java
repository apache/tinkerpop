package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapComputerMapReduce implements MapReduce {

    public static final String SIDE_EFFECT_CAP_STEP_VARIABLE = "gremlin.sideEffectCapStep.variable";

    private String variable;
    private Traversal traversal;

    public SideEffectCapComputerMapReduce() {

    }

    public SideEffectCapComputerMapReduce(final SideEffectCapComputerStep step) {
        this.variable = step.getVariable();
        this.traversal = step.getTraversal();
    }

    @Override
    public void stageConfiguration(final Configuration configuration) {
        configuration.setProperty(SIDE_EFFECT_CAP_STEP_VARIABLE, this.variable);
    }

    @Override
    public void setup(final Configuration configuration) {
        try {
            this.variable = configuration.getString(SIDE_EFFECT_CAP_STEP_VARIABLE);
            this.traversal = ((SSupplier<Traversal>) VertexProgramHelper.deserialize(configuration, TraversalVertexProgram.TRAVERSAL_SUPPLIER)).get();
            this.traversal.strategies().applyFinalStrategies();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String getResultVariable() {
        return variable;
    }

    @Override
    public boolean doReduce() {
        return false;
    }

    @Override
    public boolean doMap() {
        return false;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter emitter) {

    }

    @Override
    public Object getResult(final Iterator keyValues) {
        final Object result = ((MapReducer) ((Stream<Step>) this.traversal.getSteps().stream())
                .filter(step -> step instanceof MapReducer)
                .filter(step -> !(step instanceof SideEffectCapComputerStep))
                .filter(step -> step instanceof SideEffectCapable)
                .filter(step -> ((SideEffectCapable) step).getVariable().equals(this.variable))
                .findFirst().get())
                .getMapReduce()
                .getResult(keyValues);
        return result;
    }
}