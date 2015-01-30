package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;
import com.tinkerpop.gremlin.process.util.traversal.TraversalUtil;
import com.tinkerpop.gremlin.process.util.tool.TraverserSet;
import com.tinkerpop.gremlin.process.util.traversal.lambda.OneTraversal;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleStep<S> extends CollectingBarrierStep<S> implements Reversible, TraversalHolder {

    private Traversal.Admin<S, Number> probabilityTraversal = OneTraversal.instance();
    private final int amountToSample;
    private static final Random RANDOM = new Random();

    public SampleStep(final Traversal traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
        SampleStep.generatePredicate(this);
    }

    @Override
    public List<Traversal.Admin<S, Number>> getLocalTraversals() {
        return Collections.singletonList(this.probabilityTraversal);
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> probabilityTraversal) {
        this.probabilityTraversal = (Traversal.Admin<S, Number>) probabilityTraversal;
        this.executeTraversalOperations(this.probabilityTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.amountToSample, this.probabilityTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        requirements.add(TraverserRequirement.BULK);
        return requirements;
    }

    @Override
    public SampleStep<S> clone() throws CloneNotSupportedException {
        final SampleStep<S> clone = (SampleStep<S>) super.clone();
        clone.probabilityTraversal = this.probabilityTraversal.clone();
        clone.executeTraversalOperations(clone.probabilityTraversal, TYPICAL_LOCAL_OPERATIONS);
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
                totalWeight = totalWeight + TraversalUtil.function(s.asAdmin(), sampleStep.probabilityTraversal).doubleValue() * s.bulk();
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
                        final double currentWeight = TraversalUtil.function(s, sampleStep.probabilityTraversal).doubleValue();
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
