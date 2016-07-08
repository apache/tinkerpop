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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public class PathUtil {

    public static Set<String> getReferencedLabels(Traversal.Admin<?, ?> traversal) {
        final Set<String> referencedLabels = new HashSet<>();
        for(final Step<?, ?> step : traversal.getSteps()) {
            referencedLabels.addAll(getReferencedLabels(step));
        }
        return referencedLabels;
    }

    public static Set<String> getReferencedLabels(Step step) {
        final Set<String> referencedLabels = new HashSet<>();

        if (step instanceof Parameterizing) {
            Parameters parameters = ((Parameterizing) step).getParameters();
            for (Traversal.Admin trav : parameters.getTraversals()) {
                for (Object ss : trav.getSteps()) {
                    if (ss instanceof Scoping) {
                        Set<String> labels = ((Scoping) ss).getScopeKeys();
                        for (String label : labels) {
                            referencedLabels.add(label);
                        }
                    }
                }
            }
        }

        if (step instanceof Scoping) {
            Set<String> labels = new HashSet<>(((Scoping) step).getScopeKeys());
            if (step instanceof MatchStep) {
                // if this is the last step, keep everything, else just add founds
                if (step.getNextStep() instanceof EmptyStep) {
                    labels.addAll(((MatchStep) step).getMatchEndLabels());
                    labels.addAll(((MatchStep) step).getMatchStartLabels());
                }
            }
            referencedLabels.addAll(labels);

        }

        return referencedLabels;
    }
}
