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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalMatcher {

    private TraversalMatcher() {
    }

    public static boolean is_g_V_count(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        return steps.size() == 2 &&
                steps.get(0).getLabels().isEmpty() &&
                steps.get(1).getLabels().isEmpty() &&
                (steps.get(1) instanceof CountGlobalStep) &&
                traversal.getSteps().get(0) instanceof GraphStep &&
                ((GraphStep) traversal.getSteps().get(0)).returnsVertex() &&
                ((GraphStep) traversal.getSteps().get(0)).getIds().length == 0;
    }
}
