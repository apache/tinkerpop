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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

/**
 * A k-way merge of sorted {@link SpillRun}s. Each run has a read buffer that is reserved from the budget until
 * {@link #close()}. Ties go to the earlier run, so the merge is stable when runs are passed in creation order.
 */
final class Merger<T> implements AutoCloseable {

    private final class Head {
        final int index;
        final SpillRun.Reader reader;
        T value;
        byte[] payload;

        Head(final int index, final SpillRun.Reader reader) {
            this.index = index;
            this.reader = reader;
        }

        boolean load() {
            payload = reader.next();
            if (payload == null) return false;
            value = parser.apply(payload);
            return true;
        }
    }

    private final Function<byte[], T> parser;
    private final List<SpillRun.Reader> readers = new ArrayList<>();
    private final PriorityQueue<Head> queue;
    private Head last;
    private T value;
    private byte[] payload;

    Merger(final CsrExecutionContext ctx, final List<SpillRun> runs, final String owner, final int bufferBytes,
           final Function<byte[], T> parser, final Comparator<? super T> comparator) {
        this.parser = parser;
        this.queue = new PriorityQueue<>(Math.max(1, runs.size()), (a, b) -> {
            final int c = comparator.compare(a.value, b.value);
            return c != 0 ? c : Integer.compare(a.index, b.index);
        });
        try {
            for (int i = 0; i < runs.size(); i++) {
                final SpillRun.Reader reader = new SpillRun.Reader(ctx, runs.get(i), owner, bufferBytes);
                readers.add(reader);
                final Head head = new Head(i, reader);
                if (head.load()) queue.add(head);
            }
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Moves to the smallest remaining record.
     *
     * @return false when all runs are exhausted
     */
    boolean advance() {
        if (last != null) {
            if (last.load()) queue.add(last);
            last = null;
        }
        final Head head = queue.poll();
        if (head == null) return false;
        last = head;
        value = head.value;
        payload = head.payload;
        return true;
    }

    T value() {
        return value;
    }

    byte[] payload() {
        return payload;
    }

    @Override
    public void close() {
        for (final SpillRun.Reader r : readers) r.close();
        readers.clear();
        queue.clear();
    }

    /**
     * Merges groups of runs into longer runs until at most {@code fanIn} are left. The merged runs replace the inputs
     * in creation order, and the inputs are deleted.
     */
    static <T> List<SpillRun> reduce(final CsrExecutionContext ctx, final List<SpillRun> runs, final String owner,
                                     final String name, final Function<byte[], T> parser,
                                     final Comparator<? super T> comparator) {
        List<SpillRun> current = new ArrayList<>(runs);
        int pass = 0;
        while (true) {
            final int[] plan = SpillSupport.mergePlan(ctx);
            if (current.size() <= plan[0]) return current;
            final List<SpillRun> next = new ArrayList<>();
            for (int from = 0; from < current.size(); from += plan[0]) {
                final List<SpillRun> group = current.subList(from, Math.min(current.size(), from + plan[0]));
                if (group.size() == 1) {
                    next.add(group.get(0));
                    continue;
                }
                final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner, name + "-m" + pass, plan[1]);
                try (Merger<T> merger = new Merger<>(ctx, group, owner, plan[1], parser, comparator)) {
                    while (merger.advance()) writer.writeRecord(merger.payload());
                } catch (RuntimeException e) {
                    writer.abort();
                    throw e;
                }
                next.add(writer.finish());
                for (final SpillRun r : group) r.discard(ctx.scratch());
                ctx.checkInterrupt();
            }
            current = next;
            pass++;
        }
    }
}
