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

import org.javatuples.Triplet;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for "TraversalExplanation" instances and centralizes the key functionality which is the job of doing
 * {@link #prettyPrint()}.
 * 
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractExplanation {

    protected abstract Stream<String> getStrategyTraversalsAsString();

    protected Stream<String> getTraversalStepsAsString() {
        return Stream.concat(Stream.of(this.getOriginalTraversalAsString()), getIntermediates().map(Triplet::getValue2));
    }

    protected abstract String getOriginalTraversalAsString();

    /**
     * First string is the traversal strategy, the second is the category and the third is the traversal
     * representation at that point.
     */
    protected abstract Stream<Triplet<String,String,String>> getIntermediates();

    @Override
    public String toString() {
        return this.prettyPrint(Integer.MAX_VALUE);
    }

    public String prettyPrint() {
        return this.prettyPrint(100);
    }

    /**
     * A pretty-print representation of the traversal explanation.
     *
     * @return a {@link String} representation of the traversal explanation
     */
    public String prettyPrint(final int maxLineLength) {
        final String originalTraversal = "Original Traversal";
        final String finalTraversal = "Final Traversal";
        final int maxStrategyColumnLength = getStrategyTraversalsAsString()
                .map(String::length)
                .max(Comparator.naturalOrder())
                .orElse(15);
        final int newLineIndent = maxStrategyColumnLength + 10;
        final int maxTraversalColumn = maxLineLength - newLineIndent;
        if (maxTraversalColumn < 1)
            throw new IllegalArgumentException("The maximum line length is too small to present the " + TraversalExplanation.class.getSimpleName() + ": " + maxLineLength);
        int largestTraversalColumn = getTraversalStepsAsString()
                .map(s -> wordWrap(s, maxTraversalColumn, newLineIndent))
                .flatMap(s -> Stream.of(s.split("\n")))
                .map(String::trim)
                .map(s -> s.trim().startsWith("[") ? s : "   " + s) // 3 indent on new lines
                .map(String::length)
                .max(Comparator.naturalOrder())
                .get();

        final StringBuilder builder = new StringBuilder("Traversal Explanation\n");
        for (int i = 0; i < (maxStrategyColumnLength + 7 + largestTraversalColumn); i++) {
            builder.append("=");
        }

        spacing(originalTraversal, maxStrategyColumnLength, builder);

        builder.append(wordWrap(getOriginalTraversalAsString(), maxTraversalColumn, newLineIndent));
        builder.append("\n\n");

        final List<Triplet<String,String,String>> intermediates = this.getIntermediates().collect(Collectors.toList());
        for (Triplet<String,String,String> t : intermediates) {
            builder.append(t.getValue0());
            int spacesToAdd = maxStrategyColumnLength - t.getValue0().length() + 1;
            for (int i = 0; i < spacesToAdd; i++) {
                builder.append(" ");
            }
            builder.append("[").append(t.getValue1().substring(0, 1)).append("]");
            for (int i = 0; i < 3; i++) {
                builder.append(" ");
            }
            builder.append(wordWrap(t.getValue2(), maxTraversalColumn, newLineIndent)).append("\n");
        }

        spacing(finalTraversal, maxStrategyColumnLength, builder);

        builder.append(wordWrap((intermediates.size() > 0 ?
                intermediates.get(intermediates.size() - 1).getValue2() :
                getOriginalTraversalAsString()), maxTraversalColumn, newLineIndent));
        return builder.toString();
    }

    public static void spacing(String finalTraversal, int maxStrategyColumnLength, StringBuilder builder) {
        builder.append("\n");
        builder.append(finalTraversal);
        for (int i = 0; i < maxStrategyColumnLength - finalTraversal.length() + 7; i++) {
            builder.append(" ");
        }
    }

    private String wordWrap(final String longString, final int maxLengthPerLine, final int newLineIndent) {
        if (longString.length() <= maxLengthPerLine)
            return longString;

        StringBuilder builder = new StringBuilder();
        int counter = 0;
        for (int i = 0; i < longString.length(); i++) {
            if (0 == counter) {
                builder.append(longString.charAt(i));
            } else if (counter < maxLengthPerLine) {
                builder.append(longString.charAt(i));
            } else {
                builder.append("\n");
                for (int j = 0; j < newLineIndent; j++) {
                    builder.append(" ");
                }
                builder.append(longString.charAt(i));
                counter = 0;
            }
            counter++;
        }

        return builder.toString();
    }
}
