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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a failure raised by the {@link GraphTraversal#fail()} step which forces a traversal to immediately stop
 * in an error state. Implementations carry the contextual information about where and why the failure occurred so that
 * it can be inspected programmatically or rendered for display by way of {@link #format()}.
 * <p/>
 * When a {@code fail()} occurs in embedded mode the full context (the offending {@link Traverser} and {@link Traversal})
 * is available. When reconstructed from a remote server response, that context is not transmitted and the relevant
 * accessors may return {@code null}. Implementations and callers should account for that possibility.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Failure {

    static Translator.ScriptTranslator TRANSLATOR = GroovyTranslator.of("");

    /**
     * Gets the message associated with the failure as provided to the {@code fail()} step.
     */
    String getMessage();

    /**
     * Gets any additional metadata associated with the failure. Returns an empty {@code Map} when there is no metadata.
     */
    Map<String,Object> getMetadata();

    /**
     * Gets the {@link Traverser} that was being processed when the failure was triggered. May return {@code null} when
     * the {@code Failure} was reconstructed from a remote response where this context is not available.
     */
    Traverser.Admin getTraverser();

    /**
     * Gets the {@link Traversal} that contained the {@code fail()} step that triggered the failure. May return
     * {@code null} when the {@code Failure} was reconstructed from a remote response where this context is not
     * available.
     */
    Traversal.Admin getTraversal();

    /**
     * Gets the {@code Failure} as a formatted string representation.
     */
    public default String format() {
        final List<String> lines = new ArrayList<>();

        // some Failure implementations - notably those constructed from a remote response such as
        // FailResponseException - do not carry the traversal/traverser context as that data is not transmitted
        // from the server. guard against null returns so that format() degrades gracefully rather than throwing
        // a NullPointerException.
        final Traversal.Admin traversal = getTraversal();
        final Traverser.Admin traverser = getTraverser();
        final Step parentStep = traversal != null ? (Step) traversal.getParent() : null;

        lines.add(String.format("Message  > %s", getMessage()));

        // not sure how you'd have one without the other really
        if (traverser != null) {
            lines.add(String.format("Traverser> %s", traverser.toString()));

            if (traversal != null) {
                final TraverserGenerator generator = traversal.getTraverserGenerator();
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
            }
        }

        if (traversal != null) {
            // removes the starting period so that "__.out()" simply presents as "out()"
            lines.add(String.format("Traversal> %s", TRANSLATOR.translate(traversal).getScript().substring(1)));

            // not sure there is a situation where fail() would be used where it was not wrapped in a parent,
            // but on the odd case that it is it can be handled
            if (parentStep != null && parentStep != EmptyStep.instance()) {
                lines.add(String.format("Parent   > %s [%s]",
                        parentStep.getClass().getSimpleName(), TRANSLATOR.translate(parentStep.getTraversal()).getScript().substring(1)));
            }
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
