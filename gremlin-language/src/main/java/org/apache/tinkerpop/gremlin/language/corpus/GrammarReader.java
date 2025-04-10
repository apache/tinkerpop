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
package org.apache.tinkerpop.gremlin.language.corpus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads ANTLR grammar file to extract Gremlin keyword string representations.
 */
public class GrammarReader {

    // Pattern to match token definitions like: K_ADDALL: 'addAll';
    private static final Pattern tokenPattern = Pattern.compile("K_[A-Z0-9_]+:\\s*'([^']+)'\\s*;");

    /**
     * Parse a grammar file to extract all keyword string representations.
     *
     * @param grammarFilePath The path to the ANTLR grammar file
     * @return A set of keyword string representations
     * @throws IOException If there's an error reading the file
     */
    public static Set<String> parse(final String grammarFilePath) throws IOException {
        final Set<String> keywords = new LinkedHashSet<>();

        final List<String> lines = Files.readAllLines(Paths.get(grammarFilePath), StandardCharsets.UTF_8);

        for (String line : lines) {
            final Matcher matcher = tokenPattern.matcher(line.trim());
            if (matcher.matches()) {
                // Extract the string representation inside the single quotes
                final String keyword = matcher.group(1);
                keywords.add(keyword);
            }
        }

        return keywords;
    }
}