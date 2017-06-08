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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleGlobalStep<S> extends CollectingBarrierStep<S> implements TraversalParent, ByModulating {

    private Traversal.Admin<S, Number> probabilityTraversal = new ConstantTraversal<>(1.0d);
    private final int amountToSample;
    private static final Random RANDOM = new Random();

    public SampleGlobalStep(final Traversal.Admin traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
    }

    @Override
    public List<Traversal.Admin<S, Number>> getLocalChildren() {
        return Collections.singletonList(this.probabilityTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> probabilityTraversal) {
        this.probabilityTraversal = this.integrateChild(probabilityTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.probabilityTraversal && this.probabilityTraversal.equals(oldTraversal))
            this.probabilityTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.amountToSample, this.probabilityTraversal);
    }

    @Override
    public void processAllStarts() {
        while (this.starts.hasNext()) {
            this.traverserSet.add(this.createProjectedTraverser(this.starts.next()));
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
        final TraverserSet<S> sampledSet = new TraverserSet<>();
        int runningAmountToSample = 0;
        while (runningAmountToSample < this.amountToSample) {
            boolean reSample = false;
            double runningWeight = 0.0d;
            for (final Traverser.Admin<S> s : traverserSet) {
                long sampleBulk = sampledSet.contains(s) ? sampledSet.get(s).bulk() : 0;
                if (sampleBulk < s.bulk()) {
                    final double currentWeight = ((ProjectedTraverser<S, Number>) s).getProjections().get(0).doubleValue();
                    for (int i = 0; i < (s.bulk() - sampleBulk); i++) {
                        runningWeight = runningWeight + currentWeight;
                        if (RANDOM.nextDouble() <= ((runningWeight / totalWeight))) {
                            final Traverser.Admin<S> split = s.split();
                            split.setBulk(1L);
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
    }


    private final ProjectedTraverser<S, Number> createProjectedTraverser(final Traverser.Admin<S> traverser) {
        return new ProjectedTraverser<>(traverser, Collections.singletonList(TraversalUtil.apply(traverser, this.probabilityTraversal)));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    @Override
    public SampleGlobalStep<S> clone() {
        final SampleGlobalStep<S> clone = (SampleGlobalStep<S>) super.clone();
        clone.probabilityTraversal = this.probabilityTraversal.clone();
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
