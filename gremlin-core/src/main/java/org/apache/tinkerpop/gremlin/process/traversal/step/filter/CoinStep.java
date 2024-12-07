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
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * A filter step that will randomly allow traversers to pass through the step based on a probability value.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CoinStep<S> extends FilterStep<S> implements Seedable {

    private final Random random = new Random();
    private final GValue<Double> probability;

    public CoinStep(final Traversal.Admin traversal, final double probability) {
        super(traversal);
        this.probability = GValue.ofDouble(null, probability);
    }

    public CoinStep(final Traversal.Admin traversal, final GValue<Double> probability) {
        super(traversal);
        this.probability = null == probability ? GValue.ofDouble(null,null) : probability;
    }

    @Override
    public void resetSeed(final long seed) {
        random.setSeed(seed);
    }

    public double getProbability() {
        return probability.get();
    }

    public GValue<Double> getProbabilityGValue() {
        return probability;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        long newBulk = 0l;
        if (traverser.bulk() < 100) {
            for (int i = 0; i < traverser.bulk(); i++) {
                if (this.probability.get() >= random.nextDouble())
                    newBulk++;
            }
        } else {
            newBulk = Math.round(this.probability.get() * traverser.bulk());
        }
        if (0 == newBulk) return false;
        traverser.setBulk(newBulk);
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.probability);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Double.hashCode(this.probability.get());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }
}
