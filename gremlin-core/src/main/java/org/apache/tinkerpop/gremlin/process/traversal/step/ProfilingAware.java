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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;

import java.util.NoSuchElementException;

/**
 * Marks a {@link Step} as one that is aware of profiling. A good example of where this is important is with
 * {@link GroupStep} which needs to track and hold a {@link Barrier}, which is important for the
 * {@link ProfileStrategy} to know about. Once the {@link ProfileStep} is injected the {@link Barrier} needs to be
 * recalculated so that the timer can be properly started on the associated {@link ProfileStep}. Without that indirect
 * start of the timer, the operation related to the {@link Barrier} will not be properly accounted for and when
 * metrics are normalized it is possible to end up with a negative timing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ProfilingAware {

    /**
     * Prepares the step for any internal changes that might help ensure that profiling will work as expected.
     */
    public void prepareForProfiling();

    /**
     * A helper class which holds a {@link Barrier} and it's related {@link ProfileStep} so that the latter can have
     * its timer started and stopped appropriately.
     */
    public static final class ProfiledBarrier implements Barrier {

        private final Barrier barrier;
        private final ProfileStep profileStep;

        public ProfiledBarrier(final Barrier barrier, final ProfileStep profileStep) {
            this.barrier = barrier;
            this.profileStep = profileStep;
        }

        @Override
        public void processAllStarts() {
            this.profileStep.start();
            this.barrier.processAllStarts();
            this.profileStep.stop();
        }

        @Override
        public boolean hasNextBarrier() {
            this.profileStep.start();
            final boolean b = this.barrier.hasNextBarrier();
            this.profileStep.stop();
            return b;
        }

        @Override
        public Object nextBarrier() throws NoSuchElementException {
            this.profileStep.start();
            final Object o = this.barrier.nextBarrier();
            this.profileStep.stop();
            return o;
        }

        @Override
        public void addBarrier(final Object barrier) {
            this.barrier.addBarrier(barrier);
        }

        @Override
        public MemoryComputeKey getMemoryComputeKey() {
            return this.barrier.getMemoryComputeKey();
        }

        @Override
        public void done() {
            this.barrier.done();
        }
    }
}
