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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PathUtil {

    private PathUtil() {
        // static public methods only
    }

    public static Set<String> getReferencedLabelsAfterStep(Step<?, ?> step) {
        final Set<String> labels = new HashSet<>();
        while (!(step instanceof EmptyStep)) {
            labels.addAll(PathUtil.getReferencedLabels(step));
            step = step.getNextStep();
        }
        return labels;
    }

    public static Set<String> getReferencedLabels(final Step step) {
        final Set<String> referencedLabels = new HashSet<>();

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
