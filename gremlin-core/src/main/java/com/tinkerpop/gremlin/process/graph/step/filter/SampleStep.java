package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.util.function.CloneableLambda;
import com.tinkerpop.gremlin.util.function.ResettableLambda;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleStep<S> extends CollectingBarrierStep<S> implements Reversible, FunctionHolder<S, Number> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private Function<S, Number> probabilityFunction = s -> 1.0d;
    private final int amountToSample;
    private static final Random RANDOM = new Random();
    private boolean traversalFunction = false;

    public SampleStep(final Traversal traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
        SampleStep.generatePredicate(this);
    }


    @Override
    public void addFunction(final Function<S, Number> function) {
        this.probabilityFunction = (this.traversalFunction = function instanceof TraversalObjectLambda) ?
                ((TraversalObjectLambda) function).asTraversalLambda() :
                function;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.amountToSample);
    }

    @Override
    public List<Function<S, Number>> getFunctions() {
        return Collections.singletonList(this.probabilityFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public void reset() {
        super.reset();
        ResettableLambda.resetOrReturn(this.probabilityFunction);
    }

    @Override
    public SampleStep<S> clone() throws CloneNotSupportedException {
        final SampleStep<S> clone = (SampleStep<S>) super.clone();
        clone.probabilityFunction = CloneableLambda.cloneOrReturn(this.probabilityFunction);
        SampleStep.generatePredicate(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generatePredicate(final SampleStep<S> sampleStep) {
        sampleStep.setConsumer(traverserSet -> {
            // return the entire traverser set if the set is smaller than the amount to sample
            if (traverserSet.bulkSize() <= sampleStep.amountToSample)
                return;
            //////////////// else sample the set
            double totalWeight = 0.0d;
            for (final Traverser<S> s : traverserSet) {
                totalWeight = totalWeight + (sampleStep.probabilityFunction.apply(sampleStep.traversalFunction ? (S) s : s.get()).doubleValue() * s.bulk());
            }
            ///////
            final TraverserSet<S> sampledSet = new TraverserSet<>();
            int runningAmountToSample = 0;
            while (runningAmountToSample < sampleStep.amountToSample) {
                boolean reSample = false;
                double runningWeight = 0.0d;
                for (final Traverser.Admin<S> s : traverserSet) {
                    long sampleBulk = sampledSet.contains(s) ? sampledSet.get(s).bulk() : 0;
                    if (sampleBulk < s.bulk()) {
                        final double currentWeight = sampleStep.probabilityFunction.apply(sampleStep.traversalFunction ? (S) s : s.get()).doubleValue();
                        for (int i = 0; i < (s.bulk() - sampleBulk); i++) {
                            runningWeight = runningWeight + currentWeight;
                            if (RANDOM.nextDouble() <= (runningWeight / totalWeight)) {
                                final Traverser.Admin<S> split = s.asAdmin().split();
                                split.asAdmin().setBulk(1l);
                                sampledSet.add(split);
                                runningAmountToSample++;
                                totalWeight = totalWeight - currentWeight;
                                reSample = true;
                                break;
                            }
                        }
                        if (reSample || (runningAmountToSample >= sampleStep.amountToSample))
                            break;
                    }
                }
            }
            traverserSet.clear();
            traverserSet.addAll(sampledSet);
        });
    }
}
