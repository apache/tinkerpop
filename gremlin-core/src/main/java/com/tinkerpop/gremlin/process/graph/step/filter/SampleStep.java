package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleStep<S> extends BarrierStep<S> implements Reversible, FunctionHolder<S, Number> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private Function<S, Number> probabilityFunction = s -> 1.0d;
    private final int amountToSample;
    private static final Random RANDOM = new Random();

    public SampleStep(final Traversal traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
        super.setConsumer(traverserSet -> {
            // return the entire traverser set if the set is smaller than the amount to sample
            if (traverserSet.bulkSize() <= this.amountToSample)
                return;
            //////////////// else sample the set
            double totalWeight = 0.0d;
            for (final Traverser<S> s : traverserSet) {
                totalWeight = totalWeight + (this.probabilityFunction.apply(s.get()).doubleValue() * s.bulk());
            }
            ///////
            final TraverserSet<S> sampledSet = new TraverserSet<>();
            int runningAmountToSample = 0;
            while (runningAmountToSample < this.amountToSample) {
                boolean reSample = false;
                double runningWeight = 0.0d;
                for (final Traverser.Admin<S> s : traverserSet) {
                    long sampleBulk = sampledSet.contains(s) ? sampledSet.get(s).bulk() : 0;
                    if (sampleBulk < s.bulk()) {
                        final double currentWeight = this.probabilityFunction.apply(s.get()).doubleValue();
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
                        if (reSample || (runningAmountToSample >= this.amountToSample))
                            break;
                    }
                }
            }
            traverserSet.clear();
            traverserSet.addAll(sampledSet);
        });
    }


    @Override
    public void addFunction(final Function<S, Number> function) {
        this.probabilityFunction = function;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.amountToSample);
    }

    @Override
    public List<Function<S, Number>> getFunctions() {
        return Arrays.asList(this.probabilityFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
