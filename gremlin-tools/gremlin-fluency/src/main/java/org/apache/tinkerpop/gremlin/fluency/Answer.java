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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * What a {@link Driver} returned for one prompt.
 */
public final class Answer {
    private final String traversal;
    private final String detail;
    private final long elapsedMillis;
    private final Map<String, Long> usage;
    private final List<String> tools;

    public Answer(final String traversal, final String detail, final long elapsedMillis) {
        this(traversal, detail, elapsedMillis, Collections.emptyMap());
    }

    /**
     * @param usage what the driver reported using, such as {@code input_tokens}, {@code output_tokens} and
     *              {@code turns}, or empty if it reported nothing
     */
    public Answer(final String traversal, final String detail, final long elapsedMillis, final Map<String, Long> usage) {
        this(traversal, detail, elapsedMillis, usage, null);
    }

    /**
     * @param tools the tools the agent called, in order, or {@code null} if the driver did not report them
     */
    public Answer(final String traversal, final String detail, final long elapsedMillis, final Map<String, Long> usage,
                  final List<String> tools) {
        this.traversal = traversal;
        this.detail = detail;
        this.elapsedMillis = elapsedMillis;
        this.usage = Collections.unmodifiableMap(usage);
        this.tools = null == tools ? null : Collections.unmodifiableList(tools);
    }

    public static Answer of(final String traversal) {
        return new Answer(traversal, null, 0);
    }

    /**
     * The candidate traversal, or {@code null} when the driver produced none.
     */
    public String getTraversal() {
        return traversal;
    }

    /**
     * Why there is no traversal, or diagnostic output from the driver, if any.
     */
    public String getDetail() {
        return detail;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    /**
     * What the driver reported using, empty when it reported nothing, which is not the same as using nothing.
     */
    public Map<String, Long> getUsage() {
        return usage;
    }

    /**
     * The tools the agent called, in order, or {@code null} when the driver did not report them, which is not the same
     * as an empty list, where it reported calling none.
     */
    public List<String> getTools() {
        return tools;
    }
}
