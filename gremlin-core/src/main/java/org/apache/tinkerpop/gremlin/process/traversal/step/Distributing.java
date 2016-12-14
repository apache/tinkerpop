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
public interface Distributing {

    /**
     * Some steps should behave different whether they are executing at the master traversal or distributed across the worker traversals.
     *
     * @param atMaster whether the step is currently executing at master
     */
    public void setAtMaster(final boolean atMaster);

    /**
     * This static method will walk recursively traversal and set all {@link Distributing#setAtMaster(boolean)} accordingly.
     *
     * @param traversal                the traversal to recursively walk with the assumption that the provided traversal is a global traversal.
     * @param globalMasterDistributing whether global traversals should be treated as being at a master or worker step.
     * @param localMasterDistribution  whether local traversals should be treated as being at a master or worker step.
     */
    public static void configure(final Traversal.Admin<?, ?> traversal, final boolean globalMasterDistributing, final boolean localMasterDistribution) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof Distributing)
                ((Distributing) step).setAtMaster(globalMasterDistributing);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> global : ((TraversalParent) step).getGlobalChildren()) {
                    Distributing.configure(global, globalMasterDistributing, localMasterDistribution);
                }
                for (final Traversal.Admin<?, ?> local : ((TraversalParent) step).getLocalChildren()) {
                    Distributing.configure(local, localMasterDistribution, localMasterDistribution);
                }
            }
        }
    }
}
