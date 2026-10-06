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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.repeat;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * The loop levels of the {@code repeat()} regions that are currently being evaluated for one execution, innermost
 * last. A {@link RepeatOperator} pushes its {@link Frame} around every call into its body, {@code until} and
 * {@code emit} pipelines, and {@link LoopsOperator} reads the level from it. The stack is attached to the execution
 * context, which is frozen, through a weak map.
 */
final class LoopStack {

    private static final Map<CsrExecutionContext, LoopStack> STACKS = Collections.synchronizedMap(new WeakHashMap<>());

    private final List<Frame> frames = new ArrayList<>();
    private int keepState;

    static LoopStack of(final CsrExecutionContext ctx) {
        return STACKS.computeIfAbsent(ctx, c -> new LoopStack());
    }

    /**
     * The state of one loop: its name and the value {@code loops()} has for a traverser at this moment.
     */
    static final class Frame {
        final String name;
        int loops;

        Frame(final String name) {
            this.name = name;
        }
    }

    void push(final Frame frame) {
        frames.add(frame);
    }

    void pop() {
        frames.remove(frames.size() - 1);
    }

    /**
     * The loop count of the innermost loop (name null) or of the innermost loop of that name.
     */
    int loops(final String name) {
        if (frames.isEmpty()) throw new IllegalStateException("loops() can only be used inside a fused repeat()");
        if (name == null) return frames.get(frames.size() - 1).loops;
        for (int i = frames.size() - 1; i >= 0; i--) {
            final Frame frame = frames.get(i);
            if (name.equals(frame.name)) return frame.loops;
        }
        throw new IllegalArgumentException("Loop name not defined: " + name);
    }

    /**
     * Called by a repeat operator around the reset of its body pipelines between levels, so that a nested repeat
     * keeps its cross-level state (dedup) when its enclosing repeat advances to the next level.
     */
    void beginLevelReset() {
        keepState++;
    }

    void endLevelReset() {
        keepState--;
    }

    boolean keepsState() {
        return keepState > 0;
    }
}
