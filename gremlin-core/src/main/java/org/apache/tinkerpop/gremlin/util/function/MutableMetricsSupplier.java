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

package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MutableMetricsSupplier implements Supplier<MutableMetrics>, Serializable {

    private final String stepId;
    private final String stepString;

    public MutableMetricsSupplier(final Step<?, ?> previousStep) {
        this.stepId = previousStep.getId();
        this.stepString =
                previousStep instanceof RepeatStep.RepeatEndStep || previousStep instanceof ComputerAwareStep.EndStep ?
                        previousStep.toString() + " (profiling ignored)" :
                        previousStep.toString();
    }

    @Override
    public MutableMetrics get() {
        return new MutableMetrics(this.stepId, this.stepString);
    }
}
