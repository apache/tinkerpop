/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleGlobalStep<S> extends CollectingBarrierStep<S> implements TraversalParent, ByModulating, Seedable {

    private boolean detectedMutipleBy = false;
    private Traversal.Admin<S, Number> probabilityTraversal = new ConstantTraversal<>(1.0d);;
    private final int amountToSample;
    private final Random random = new Random();

    public SampleGlobalStep(final Traversal.Admin traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
    }

    public int getAmountToSample() {
        return amountToSample;
    }

    @Override
    public void resetSeed(final long seed) {
        random.setSeed(seed);
    }

    @Override
    public List<Traversal.Admin<S, Number>> getLocalChildren() {
        return Collections.singletonList(this.probabilityTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> probabilityTraversal) {
        if (this.detectedMutipleBy)
            throw new IllegalStateException("Sample step can only have one by modulator");
        this.detectedMutipleBy = true;
        this.probabilityTraversal = this.integrateChild(probabilityTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.probabilityTraversal && this.probabilityTraversal.equals(oldTraversal)) {
            this.detectedMutipleBy = true;
            this.probabilityTraversal = this.integrateChild(newTraversal);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.amountToSample, this.probabilityTraversal);
    }

    @Override
    public void processAllStarts() {
        while (this.starts.hasNext()) {
            Traverser.Admin<S> next = this.starts.next();
            //System.out.println("Adding start: " + next + " to traverserSet: " + this.traverserSet);
            this.createProjectedTraverser(next).ifPresent(traverserSet::add);
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        // return the entire traverser set if the set is smaller than the amount to sample
        if (traverserSet.bulkSize() <= this.amountToSample)
            return;
        //////////////// else sample the set
        double totalWeight = 0.0d;
        for (final Traverser.Admin<S> s : traverserSet) {
            totalWeight = totalWeight + (((ProjectedTraverser<S, Number>) s).getProjections().get(0).doubleValue() * s.bulk());
        }
        ///////
        //System.out.println("Sampling from traverserSet: " + traverserSet);
        final TraverserSet<S> sampledSet = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        int runningAmountToSample = 0;
        while (runningAmountToSample < this.amountToSample) {
            boolean reSample = false;
            double runningTotalWeight = totalWeight;
            for (final Traverser.Admin<S> s : traverserSet) {
                long sampleBulk = sampledSet.contains(s) ? sampledSet.get(s).bulk() : 0;
                if (sampleBulk < s.bulk()) {
                    final double currentWeight = ((ProjectedTraverser<S, Number>) s).getProjections().get(0).doubleValue();
                    for (int i = 0; i < (s.bulk() - sampleBulk); i++) {
                        if (random.nextDouble() <= ((currentWeight / runningTotalWeight))) {
                            final Traverser.Admin<S> split = s.split();
                            split.setBulk(1L);
                            //System.out.println("Adding sample: " + split);
                            sampledSet.add(split);
                            runningAmountToSample++;
                            reSample = true;
                            break;
                        }
                        runningTotalWeight = runningTotalWeight - currentWeight;
                    }
                    if (reSample || (runningAmountToSample >= this.amountToSample))
                        break;
                }
            }
        }
        traverserSet.clear();
        //System.out.println("SampledSet: " + sampledSet);
        traverserSet.addAll(sampledSet);
        //System.out.println("TraverserSet: " + traverserSet);
    }

    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
//        return MemoryComputeKey.of(this.getId(), (ts1, ts2) -> {
//            System.out.println("Merging traverserSets: " + ts1 + " + " + ts2);
//            return (TraverserSet<S>) Operator.addAll.apply(ts1, ts2);
//        }, false, true);
        return MemoryComputeKey.of(this.getId(), new SampleBiOperator<>(this.amountToSample), false, true);
    }

    public static final class SampleBiOperator<S> implements BinaryOperator<TraverserSet<S>>, Serializable {

        private long sampleSize;

        private SampleBiOperator() {
            // for serializers that need a no-arg constructor
        }

        public SampleBiOperator(final long sampleSize) {
            this.sampleSize = sampleSize;
        }

        @Override
        public TraverserSet<S> apply(final TraverserSet<S> setA, final TraverserSet<S> setB) {
            //System.out.println("SampleBiOperator.apply: " + setA + " -> " + setB);

            Map<Integer, TraverserSet> loopMap = new TreeMap<>(Comparator.reverseOrder());

            for (Traverser.Admin<S> traverser : setA) {
                loopMap.computeIfAbsent(traverser.loops(), integer -> new TraverserSet<>()).add(traverser);
            }
            for (Traverser.Admin<S> traverser : setB) {
                loopMap.computeIfAbsent(traverser.loops(), integer -> new TraverserSet<>()).add(traverser);
            }
            
            return loopMap.entrySet().iterator().next().getValue();

//            return (TraverserSet<S>) Operator.addAll.apply(setA, setB);
//            setA.addAll(setB);
//            if (this.sampleSize != -1 && setA.bulkSize() > this.sampleSize) {
//                long counter = 0L;
//                final Iterator<Traverser.Admin<S>> traversers = setA.iterator();
//                while (traversers.hasNext()) {
//                    final Traverser.Admin<S> traverser = traversers.next();
//                    if (counter > this.sampleSize) {
//                        System.out.println("SampleBiOperator removed traverser: " + traverser);
//                        traversers.remove();
//                    }
//                    counter = counter + traverser.bulk();
//                }
//            }
//            return setA;
        }
    }


    private Optional<ProjectedTraverser<S, Number>> createProjectedTraverser(final Traverser.Admin<S> traverser) {
        final TraversalProduct product = TraversalUtil.produce(traverser, this.probabilityTraversal);
        if (product.isProductive()) {
            final Object o = product.get();
            if (!(o instanceof Number)) {
                throw new IllegalStateException(String.format(
                        "Traverser %s does not evaluate to a number with %s", traverser, this.probabilityTraversal));
            }

            return Optional.of(new ProjectedTraverser<>(traverser, Collections.singletonList((Number) product.get())));
        } else {
            return Optional.empty();
        }

    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.NESTED_LOOP);
    }

    @Override
    public SampleGlobalStep<S> clone() {
        final SampleGlobalStep<S> clone = (SampleGlobalStep<S>) super.clone();
        clone.probabilityTraversal = this.probabilityTraversal.clone();
        clone.detectedMutipleBy = this.detectedMutipleBy;
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.probabilityTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.amountToSample ^ this.probabilityTraversal.hashCode();
    }
}