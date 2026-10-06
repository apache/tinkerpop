/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * Console-friendly inspection of native execution without {@code profile()}.
 */
public final class CsrNative {

    private CsrNative() {
    }

    /**
     * The most bytes the memory budget held at once by the native steps of the traversal in its last execution (the
     * budget is shared by the whole root traversal, so this is a traversal-wide peak), or -1 if no native step ran. It works after {@code toList()}, {@code next()} or
     * a {@code CsrMemoryBudgetException}, for example
     * {@code t = g.V().out().dedup().count(); t.next(); CsrNative.lastPeakBytes(t)}.
     */
    public static long lastPeakBytes(final Traversal<?, ?> traversal) {
        long peak = -1L;
        for (final CsrSuperStep<?, ?> step : TraversalHelper.getStepsOfAssignableClassRecursively(
                CsrSuperStep.class, traversal.asAdmin())) {
            peak = Math.max(peak, step.peakBytes());
        }
        return peak;
    }

    /**
     * The bytes the native steps of the traversal wrote to scratch files in its last execution, or -1 if no native
     * step ran; above zero means something spilled. Like {@link #lastPeakBytes} it works after {@code toList()} or a
     * {@code CsrMemoryBudgetException}, and unlike {@code profile()} it does not stop group steps from fusing.
     */
    public static long lastScratchBytes(final Traversal<?, ?> traversal) {
        long bytes = -1L;
        for (final CsrSuperStep<?, ?> step : TraversalHelper.getStepsOfAssignableClassRecursively(
                CsrSuperStep.class, traversal.asAdmin())) {
            bytes = Math.max(bytes, step.scratchBytes());
        }
        return bytes;
    }
}
