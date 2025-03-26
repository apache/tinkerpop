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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface Failure {

    String getMessage();

    Map<String,Object> getMetadata();

    Traverser.Admin getTraverser();

    Traversal.Admin getTraversal();

    /**
     * Gets the {@code Failure} as a formatted string representation.
     */
    public default String format() {
        final List<String> lines = new ArrayList<>();
        final Step parentStep = (Step) getTraversal().getParent();

        lines.add(String.format("Message  > %s", getMessage()));
        lines.add(String.format("Traverser> %s", getTraverser().toString()));

        final TraverserGenerator generator = getTraversal().getTraverserGenerator();
        final Traverser.Admin traverser = getTraverser();
        if (generator.getProvidedRequirements().contains(TraverserRequirement.BULK)) {
            lines.add(String.format("  Bulk   > %s", traverser.bulk()));
        }
        if (generator.getProvidedRequirements().contains(TraverserRequirement.SACK)) {
            lines.add(String.format("  Sack   > %s", traverser.sack()));
        }
        if (generator.getProvidedRequirements().contains(TraverserRequirement.PATH)) {
            lines.add(String.format("  Path   > %s", traverser.path()));
        }
        if (generator.getProvidedRequirements().contains(TraverserRequirement.SINGLE_LOOP) ||
                generator.getProvidedRequirements().contains(TraverserRequirement.NESTED_LOOP) ) {
            final Set<String> loopNames = traverser.getLoopNames();
            final String loopsLine = loopNames.isEmpty() ?
                    String.valueOf(traverser.asAdmin().loops()) :
                    loopNames.stream().collect(Collectors.toMap(loopName -> loopName, traverser::loops)).toString();
            lines.add(String.format("  Loops  > %s", loopsLine));
        }
        if (generator.getProvidedRequirements().contains(TraverserRequirement.SIDE_EFFECTS)) {
            final TraversalSideEffects tse = traverser.getSideEffects();
            final Set<String> keys = tse.keys();
            lines.add(String.format("  S/E    > %s", keys.stream().collect(Collectors.toMap(k -> k, tse::get))));
        }

        // removes the starting period so that "__.out()" simply presents as "out()"
        lines.add(String.format("Traversal> %s", getTraversal().getGremlinLang().getGremlin().substring(1)));

        // not sure there is a situation where fail() would be used where it was not wrapped in a parent,
        // but on the odd case that it is it can be handled
        if (parentStep != EmptyStep.instance()) {
            lines.add(String.format("Parent   > %s [%s]",
                    parentStep.getClass().getSimpleName(), getTraversal().getGremlinLang().getGremlin().substring(1)));
        }

        lines.add(String.format("Metadata > %s", getMetadata()));

        final int longestLineLength = lines.stream().mapToInt(String::length).max().getAsInt();
        final String separatorLine = String.join("", Collections.nCopies(longestLineLength, "="));
        lines.add(0, separatorLine);
        lines.add(0, "fail() Step Triggered");
        lines.add(separatorLine);

        return String.join(System.lineSeparator(), lines);
    }
}
