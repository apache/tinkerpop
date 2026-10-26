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
package org.apache.tinkerpop.gremlin.fluency;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Answers from a YAML file that maps scenario names to traversals, which replays known answers through the scorer.
 * A scenario missing from the file gets no answer.
 */
public final class ReplayDriver implements Driver {
    private final Map<String, String> traversals;

    public ReplayDriver(final Map<String, String> traversals) {
        this.traversals = new HashMap<>(traversals);
    }

    public static ReplayDriver fromFile(final Path file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            final Map<String, String> traversals = new Yaml().load(reader);
            return new ReplayDriver(null == traversals ? Collections.emptyMap() : traversals);
        }
    }

    @Override
    public Answer answer(final IntentScenario scenario, final String prompt, final int attempt) {
        final String traversal = traversals.get(scenario.getName());
        return null == traversal ? new Answer(null, "no replay answer for " + scenario.getName(), 0) : Answer.of(traversal);
    }
}
