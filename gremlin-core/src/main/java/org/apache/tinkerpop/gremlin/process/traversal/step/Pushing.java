/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pushing {

    /**
     * Set whether this step will be executing using a push-based model or the standard pull-based iterator model.
     */
    public void setPushBased(final boolean pushBased);

    /**
     * This static method will walk recursively traversal and set all {@link Pushing#setPushBased(boolean)} accordingly.
     *
     * @param traversal       the traversal to recursively walk with the assumption that the provided traversal is a global traversal.
     * @param globalPushBased the traverser propagation semantics (push or pull) of global children (typically push).
     * @param localPushBased  the traverser propagation semantics (push or pull) of local children (typically pull).
     */
    public static void configure(final Traversal.Admin<?, ?> traversal, final boolean globalPushBased, final boolean localPushBased) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof Pushing)
                ((Pushing) step).setPushBased(globalPushBased);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> global : ((TraversalParent) step).getGlobalChildren()) {
                    Pushing.configure(global, globalPushBased, localPushBased);
                }
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    Pushing.configure(local, localPushBased, localPushBased);
                }
            }
        }
    }
}
