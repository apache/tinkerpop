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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;

import java.util.NoSuchElementException;

/**
 * A Barrier is any step that requires all left traversers to be processed prior to emitting result traversers to the right.
 * Note that some barrier steps may be "lazy" in that if their algorithm permits, they can emit right traversers prior to all traversers being aggregated.
 * A barrier is the means by which a distributed step in {@link TraversalVertexProgram} is synchronized and made to behave a single step.
 * All Barrier steps implement {@link MemoryComputing} as that is how barriers communicate with one another in {@link GraphComputer}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Barrier<B> extends MemoryComputing<B> {

    /**
     * Process all left traversers by do not yield the resultant output.
     * This method is useful for steps like {@link ReducingBarrierStep}, where traversers can be processed "on the fly" and thus, reduce memory consumption.
     */
    public void processAllStarts();

    /**
     * Whether or not the step has an accessible barrier.
     *
     * @return whether a barrier exists or not
     */
    public boolean hasNextBarrier();

    /**
     * Get the next barrier within this step.
     * Barriers from parallel steps can be the be merged to create a single step with merge barriers.
     *
     * @return the next barrier of the step
     * @throws NoSuchElementException
     */
    public B nextBarrier() throws NoSuchElementException;

    /**
     * Add a barrier to the step.
     * This typically happens when multiple parallel barriers need to become one barrier at a single step.
     *
     * @param barrier the barrier to merge in
     */
    public void addBarrier(final B barrier);

    /**
     * A way to hard set that the barrier is complete.
     * This is necessary when parallel barriers don't all have barriers and need hard resetting.
     * The default implementation does nothing.
     */
    public default void done() {

    }
}
