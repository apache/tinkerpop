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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The prompt a driver receives for an {@link IntentScenario}. It carries the intent only. The graph is not named, as
 * it would not be for an agent working on a real graph, so the agent learns about it only from the request or from
 * the tools it is given. The scenario name, which spells out the reference traversal, and the expected results are
 * never part of it.
 */
public final class Prompt {
    private static final String TEMPLATE = load();

    private Prompt() {}

    public static String render(final IntentScenario scenario) {
        return TEMPLATE.replace("{intent}", scenario.getIntent());
    }

    /**
     * Identifies the template so that runs made with different prompts are not compared as if they were alike.
     */
    public static String templateHash() {
        return Hashes.of(TEMPLATE);
    }

    private static String load() {
        try (InputStream in = Prompt.class.getResourceAsStream("prompt.txt")) {
            if (null == in) throw new IllegalStateException("Missing prompt.txt");
            // the license header and other comments are for maintainers, not the agent
            final String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return Stream.of(text.split("\\n", -1)).filter(line -> !line.startsWith("#")).
                    collect(Collectors.joining("\n")).trim() + "\n";
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
