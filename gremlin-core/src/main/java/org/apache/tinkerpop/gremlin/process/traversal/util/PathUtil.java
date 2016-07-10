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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public class PathUtil {

    private PathUtil() {
        // static public methods only
    }

    public static Set<String> getReferencedLabelsAfterStep(Step<?, ?> step) {
        if (step.getNextStep().equals(EmptyStep.instance())) {
            return Collections.emptySet();
        }
        final Set<String> labels = new HashSet<>();
        while (!(step = step.getNextStep()).equals(EmptyStep.instance())) {
            labels.addAll(PathUtil.getReferencedLabels(step));
        }
        return labels;
    }

    public static Set<String> getReferencedLabels(final Traversal.Admin<?, ?> traversal) {
        final Set<String> referencedLabels = new HashSet<>();
        for (final Step<?, ?> step : traversal.getSteps()) {
            referencedLabels.addAll(getReferencedLabels(step));
        }
        return referencedLabels;
    }

    public static Set<String> whichLabelsReferencedFromHereForward(Step<?, ?> step) {
        final Set<String> found = new HashSet<>();
        while (!step.equals(EmptyStep.instance())) {
            final Set<String> referencedLabels = getReferencedLabels(step);
            for (final String refLabel : referencedLabels) {
                found.add(refLabel);
            }
            step = step.getNextStep();
        }
        return found;
    }

    public static Set<String> getReferencedLabels(final Step step) {
        final Set<String> referencedLabels = new HashSet<>();

        if (step instanceof Parameterizing) { // TODO: we should really make the mutation steps Scoping :|
            final Parameters parameters = ((Parameterizing) step).getParameters();
            for (final Traversal.Admin trav : parameters.getTraversals()) {
                for (final Object ss : trav.getSteps()) {
                    if (ss instanceof Scoping) {
                        for (String label : ((Scoping) ss).getScopeKeys()) {
                            referencedLabels.add(label);
                        }
                    }
                }
            }
        }

        if (step instanceof Scoping) {
            final Set<String> labels = new HashSet<>(((Scoping) step).getScopeKeys());
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
