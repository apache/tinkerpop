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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@code dedup()} of a repeat body whose seen-set lives as long as the repeat instead of as long as one level, with
 * state that is bounded by the memory budget. The body pipelines are reset between levels, so the repeat cuts its body
 * at every dedup and runs this stage between the pieces; the stage is cleared only when the repeat is reset (see
 * {@link #clear()}), and a level that was abandoned half way is dropped with {@link #abandonLevel()}. The behavior
 * matches {@code DedupOperator}: the first entry for each key passes with bulk 1.
 * <p>
 * Vertices and edges keyed by themselves or by {@code T.id} use a bitset over the ordinals when the quota covers it,
 * and the encoded path otherwise. Labels use a tiny bitset. Every other key is encoded ({@link KeyCodec}) and kept in
 * a hash set, reserved once per distinct key within the quota of the stage, and entries are emitted as they arrive.
 * <p>
 * When the set cannot grow, its keys are written to 16 <em>visited</em> runs, one per hash partition. From then on
 * the stage works level by level in blocking mode: the upstream of the level is drained into candidate partitions
 * (same hash), and at its end each partition is replayed in arrival order against the visited keys of its partition. An
 * entry whose key has not been seen survives and its key is appended to the visited runs of the partition, so the
 * visited set is exactly what the in-heap stage would hold. A partition whose keys do not fit is split with another
 * hash, up to {@link SpillSupport#MAX_DEPTH} times; the visited runs are not split but filtered by the hash path while
 * they are read. The survivors of all partitions are merged by arrival sequence, so the output of a level keeps its
 * order, and have bulk 1.
 * <p>
 * Record formats: {@code [type 0][keyLen][key]} visited key, {@code [type 1][keyLen][key][seq][entry]} candidate;
 * survivor record {@code [seq][entry]}.
 */
public final class SpillPersistentDedup implements BatchSupplier {

    private static final int VISITED = 0;
    private static final int CANDIDATE = 1;
    private static final int MAX_VISITED_RUNS = 8;

    private enum Mode {ORDINAL, LABEL, HASH}

    private enum Phase {STREAM, COLLECT, DRAIN}

    private final String owner;
    private final Keys.Key key;
    private final Lane lane;
    private final BatchSupplier upstream;
    private final CsrExecutionContext ctx;
    private final CsrSnapshot snapshot;
    private final Mode mode;
    private final String scratchName;

    private Batch in;
    private long[] bits;
    private int noLabelSlot;
    private boolean ordinalKeys;
    private KeyCodec codec;
    private KeyEvaluator keys;
    private EntryCodec entries;
    private SpillReserve spillReserve;
    private final Bytes canon = new Bytes();
    private final Bytes rep = new Bytes();
    private final Bytes record = new Bytes();

    private Set<KeyBytes> seen;
    private Phase phase = Phase.STREAM;
    private SpillPartitions partitions;
    private long sequence;
    @SuppressWarnings("unchecked")
    private final List<SpillRun>[] visited = new List[SpillPartitions.FANOUT];
    private final List<SpillRun> survivors = new ArrayList<>();
    private Merger<Long> merger;
    private boolean endOfLevel;

    public SpillPersistentDedup(final CsrExecutionContext ctx, final String owner, final Ops.Dedup node,
                                final Lane lane, final BatchSupplier upstream) {
        this.ctx = ctx;
        this.owner = owner;
        this.key = node.key();
        this.lane = lane;
        this.upstream = upstream;
        this.snapshot = ctx.snapshot();
        this.scratchName = owner.replace(' ', '_');
        final boolean element = lane.isElement();
        if (element && (key instanceof Keys.Identity || (key instanceof Keys.Token t && t.token() == T.id))) {
            mode = Mode.ORDINAL;
        } else if (element && key instanceof Keys.Token t && t.token() == T.label) {
            mode = Mode.LABEL;
        } else {
            mode = Mode.HASH;
            if (key instanceof Keys.Token && !element) {
                throw new UnsupportedOperationException("A dedup by a token of " + lane + " inside a fused repeat() is not supported");
            }
        }
        for (int p = 0; p < visited.length; p++) visited[p] = new ArrayList<>();
        boolean opened = false;
        try {
            if (mode == Mode.ORDINAL) {
                final long words = ((lane == Lane.V ? snapshot.vertexCount() : snapshot.edgeCount()) + 63L) >>> 6;
                if (ctx.tryReserveWithinQuota(8 * words, owner + " seen bits")) bits = new long[(int) words];
                else ordinalKeys = true;
            } else if (mode == Mode.LABEL) {
                final int labels = lane == Lane.V ? snapshot.vertexLabels().size() : snapshot.edgeLabels().size();
                if (lane == Lane.V) {
                    final int empty = snapshot.vertexLabelCodeOf("");
                    noLabelSlot = empty >= 0 ? empty : labels;
                }
                final long words = ((long) labels + 1 + 63) >>> 6;
                ctx.budget().reserve(8 * words, owner + " seen bits");
                bits = new long[(int) words];
            }
            if (bits == null) {
                // the encoded path
                spillReserve = new SpillReserve(ctx, owner + " spill reserve", SpillReserve.PARTITIONS);
                spillReserve.hold();
                codec = new KeyCodec(ctx);
                entries = new EntryCodec(codec, lane, false);
                if (!ordinalKeys) {
                    keys = new KeyEvaluator(ctx, codec, key, lane);
                    keys.open();
                }
                seen = new HashSet<>();
            }
            opened = true;
        } finally {
            if (!opened) close();
        }
    }

    // ---------------------------------------------------------------- the stage

    @Override
    public boolean next(final Batch out) {
        out.clear();
        if (endOfLevel) {
            endOfLevel = false;
            return false;
        }
        if (phase == Phase.DRAIN) return drain(out);
        if (in == null) in = new Batch(lane, out.capacity);
        while (true) {
            if (!upstream.next(in)) {
                if (phase == Phase.COLLECT && partitions != null) {
                    finishLevel();
                    return drain(out);
                }
                return out.n > 0;
            }
            for (int i = 0; i < in.n; i++) {
                if (bits != null) bitEntry(i, out);
                else if (phase == Phase.STREAM) streamEntry(i, out);
                else collectEntry(i);
            }
            if (phase == Phase.STREAM && out.n > 0) return true;
            ctx.checkInterrupt();
        }
    }

    private void bitEntry(final int i, final Batch out) {
        final int slot = mode == Mode.ORDINAL ? in.ord[i] : labelSlot(in.ord[i]);
        final int word = slot >>> 6;
        final long mask = 1L << (slot & 63);
        if ((bits[word] & mask) == 0) {
            bits[word] |= mask;
            out.copyEntry(in, i, 1L);
        }
    }

    private int labelSlot(final int ordinal) {
        if (lane == Lane.E) return snapshot.edgeLabelCode(ordinal);
        final int code = snapshot.vertexLabelCode(ordinal);
        return code < 0 ? noLabelSlot : code;
    }

    /**
     * Encodes the key of entry {@code i} into {@code canon}.
     *
     * @return false if the key is non-productive, which filters the entry
     */
    private boolean encodeKey(final int i) {
        canon.reset();
        if (ordinalKeys) {
            canon.putInt(in.ord[i]);
            return true;
        }
        if (!keys.evaluate(in, i)) return false;
        rep.reset();
        keys.encode(canon, rep);
        return true;
    }

    private void streamEntry(final int i, final Batch out) {
        if (!encodeKey(i)) return;
        final byte[] bytes = canon.toArray();
        final KeyBytes k = new KeyBytes(bytes, KeyBytes.EMPTY);
        if (seen.contains(k)) return;
        if (ctx.tryReserveWithinQuota(SpillSupport.SET_ENTRY + bytes.length, owner + " seen keys")) {
            seen.add(k);
            out.copyEntry(in, i, 1L);
            return;
        }
        startSpill();
        spill(i);
    }

    private void collectEntry(final int i) {
        if (encodeKey(i)) spill(i);
    }

    private SpillPartitions newPartitions(final String what) {
        spillReserve.handOver();
        return new SpillPartitions(ctx, owner + " spill buffers", scratchName + what, 0);
    }

    /**
     * Writes the seen keys to the visited runs and switches to blocking mode for the rest of the repeat.
     */
    private void startSpill() {
        final SpillPartitions marks = newPartitions("-visited");
        try {
            final Bytes marker = new Bytes();
            for (final KeyBytes k : seen) {
                marker.reset();
                marker.putByte(VISITED);
                marker.putBlock(k.canon);
                marks.append(marker.toArray());
                ctx.checkInterrupt();
            }
        } catch (RuntimeException e) {
            marks.abort();
            throw e;
        }
        final SpillRun[] runs = marks.finish();
        ctx.budget().releaseAll(owner + " spill buffers");
        for (int p = 0; p < runs.length; p++) {
            if (runs[p] != null) visited[p].add(runs[p]);
        }
        seen.clear();
        ctx.budget().releaseAll(owner + " seen keys");
        spillReserve.retake();
        phase = Phase.COLLECT;
        sequence = 0;
        partitions = newPartitions("-candidates");
    }

    private void spill(final int i) {
        if (partitions == null) partitions = newPartitions("-candidates");
        record.reset();
        record.putByte(CANDIDATE);
        record.putInt(canon.size());
        record.putBytes(canon);
        record.putLong(sequence++);
        entries.encode(in, i, 1L, record);
        partitions.append(record.toArray());
    }

    // ---------------------------------------------------------------- blocking mode

    /**
     * Replays the candidates of the level against the visited keys and sets up the merge of the survivors.
     */
    private void finishLevel() {
        final SpillRun[] runs = partitions.finish();
        partitions = null;
        ctx.budget().releaseAll(owner + " spill buffers");
        spillReserve.retake();
        for (int p = 0; p < runs.length; p++) {
            if (runs[p] != null) replay(runs[p], p, new int[]{p});
        }
        for (int p = 0; p < visited.length; p++) {
            if (visited[p].size() > MAX_VISITED_RUNS) compact(p);
        }
        final List<SpillRun> reduced = Merger.reduce(ctx, survivors, owner + " merge buffers", scratchName + "-survivors",
                SpillPersistentDedup::sequenceOf, Long::compare);
        survivors.clear();
        survivors.addAll(reduced);
        final int[] plan = SpillSupport.mergePlan(ctx);
        merger = new Merger<>(ctx, survivors, owner + " merge buffers", plan[1], SpillPersistentDedup::sequenceOf,
                Long::compare);
        phase = Phase.DRAIN;
    }

    private static Long sequenceOf(final byte[] payload) {
        return new ByteSource(payload).getLong();
    }

    private boolean drain(final Batch out) {
        while (!out.isFull() && merger.advance()) {
            entries.decode(new ByteSource(merger.payload(), 8), out);
        }
        if (!out.isFull()) {
            endLevel();
            endOfLevel = out.n > 0;
        }
        return out.n > 0;
    }

    /**
     * Drops the survivors of the level and gets ready for the next one.
     */
    private void endLevel() {
        if (merger != null) {
            merger.close();
            merger = null;
        }
        for (final SpillRun run : survivors) run.discard(ctx.scratch());
        survivors.clear();
        ctx.budget().releaseAll(owner + " merge buffers");
        sequence = 0;
        phase = Phase.COLLECT;
    }

    /**
     * Whether a key belongs to the partition path of a replay: the partition under each hash seed so far.
     */
    private static boolean onPath(final byte[] key, final int[] path) {
        for (int seed = 1; seed < path.length; seed++) {
            if (((KeyBytes.partitionHash(key, 0, key.length, seed) >>> 1) % SpillPartitions.FANOUT) != path[seed]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Replays a candidate partition against the visited keys of its coarse partition. If the keys do not fit, splits
     * the candidates and replays the parts.
     */
    private void replay(final SpillRun run, final int coarse, final int[] path) {
        final int depth = path.length - 1;
        final String setOwner = owner + " partition keys";
        final String name = scratchName + "-replay" + depth;
        final SpillRun.Writer survivorWriter = new SpillRun.Writer(ctx, owner + " survivor buffer", name + "-s",
                SpillSupport.bufferBytes(ctx, 4));
        final SpillRun.Writer visitedWriter;
        try {
            visitedWriter = new SpillRun.Writer(ctx, owner + " survivor buffer", name + "-v",
                    SpillSupport.bufferBytes(ctx, 4));
        } catch (RuntimeException e) {
            survivorWriter.abort();
            throw e;
        }
        final Set<KeyBytes> set = new HashSet<>();
        boolean overflow = false;
        try {
            // the visited keys of the partition
            for (final SpillRun old : visited[coarse]) {
                final SpillRun.Reader reader = new SpillRun.Reader(ctx, old, owner + " partition reader",
                        SpillSupport.bufferBytes(ctx, 4));
                try {
                    byte[] payload;
                    while (!overflow && (payload = reader.next()) != null) {
                        final byte[] k = new ByteSource(payload, 1).getBlock();
                        if (depth > 0 && !onPath(k, path)) continue;
                        overflow = !admit(set, k, depth, setOwner);
                        ctx.checkInterrupt();
                    }
                } finally {
                    reader.close();
                }
                if (overflow) break;
            }
            // the candidates, in arrival order
            if (!overflow) {
                final SpillRun.Reader reader = new SpillRun.Reader(ctx, run, owner + " partition reader",
                        SpillSupport.bufferBytes(ctx, 4));
                try {
                    byte[] payload;
                    final Bytes marker = new Bytes();
                    while (!overflow && (payload = reader.next()) != null) {
                        final byte[] k = new ByteSource(payload, 1).getBlock();
                        if (set.contains(new KeyBytes(k, KeyBytes.EMPTY))) continue;
                        if (!admit(set, k, depth, setOwner)) {
                            overflow = true;
                            break;
                        }
                        // the record after the key is [seq][entry]
                        survivorWriter.writeRecord(payload, 1 + 4 + k.length, payload.length - 5 - k.length);
                        marker.reset();
                        marker.putByte(VISITED);
                        marker.putBlock(k);
                        visitedWriter.writeRecord(marker.toArray());
                        ctx.checkInterrupt();
                    }
                } finally {
                    reader.close();
                }
            }
        } catch (RuntimeException e) {
            survivorWriter.abort();
            visitedWriter.abort();
            ctx.budget().releaseAll(setOwner);
            throw e;
        }
        ctx.budget().releaseAll(setOwner);
        if (!overflow) {
            final SpillRun kept = survivorWriter.finish();
            if (kept.records() > 0) survivors.add(kept);
            else kept.discard(ctx.scratch());
            final SpillRun fresh = visitedWriter.finish();
            if (fresh.records() > 0) visited[coarse].add(fresh);
            else fresh.discard(ctx.scratch());
            run.discard(ctx.scratch());
            return;
        }
        survivorWriter.abort();
        visitedWriter.abort();
        ctx.budget().releaseAll(owner + " survivor buffer");
        // the headroom goes to the partition buffers of the split
        spillReserve.handOver();
        final SpillRun[] parts = SpillPartitions.split(ctx, owner + " spill buffers", name, run, depth + 1);
        ctx.budget().releaseAll(owner + " spill buffers");
        spillReserve.retake();
        for (int q = 0; q < parts.length; q++) {
            if (parts[q] == null) continue;
            final int[] next = Arrays.copyOf(path, depth + 2);
            next[depth + 1] = q;
            replay(parts[q], coarse, next);
        }
    }

    /**
     * Adds the key to the set of a replay; at the deepest split the cost is reserved even beyond the quota.
     *
     * @return false if the key does not fit
     */
    private boolean admit(final Set<KeyBytes> set, final byte[] k, final int depth, final String setOwner) {
        final KeyBytes kb = new KeyBytes(k, KeyBytes.EMPTY);
        if (set.contains(kb)) return true;
        final long cost = SpillSupport.SET_ENTRY + k.length;
        if (depth >= SpillSupport.MAX_DEPTH) ctx.budget().reserve(cost, setOwner);
        else if (!ctx.tryReserveWithinQuota(cost, setOwner)) return false;
        set.add(kb);
        return true;
    }

    /**
     * Concatenates the visited runs of a partition into one; they hold distinct keys, so no merge is needed.
     */
    private void compact(final int p) {
        final List<SpillRun> runs = visited[p];
        final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner + " survivor buffer", scratchName + "-compact",
                SpillSupport.bufferBytes(ctx, 4));
        try {
            for (final SpillRun old : runs) {
                final SpillRun.Reader reader = new SpillRun.Reader(ctx, old, owner + " partition reader",
                        SpillSupport.bufferBytes(ctx, 4));
                try {
                    byte[] payload;
                    while ((payload = reader.next()) != null) writer.writeRecord(payload);
                } finally {
                    reader.close();
                }
                ctx.checkInterrupt();
            }
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
        final SpillRun compacted = writer.finish();
        for (final SpillRun old : runs) old.discard(ctx.scratch());
        runs.clear();
        runs.add(compacted);
    }

    // ---------------------------------------------------------------- lifecycle

    private void dropLevel() {
        if (merger != null) {
            merger.close();
            merger = null;
        }
        if (partitions != null) {
            partitions.abort();
            partitions = null;
        }
        for (final SpillRun run : survivors) run.discard(ctx.scratch());
        survivors.clear();
        for (final String state : new String[]{"spill buffers", "merge buffers", "survivor buffer", "partition reader",
                "partition keys"}) {
            ctx.budget().releaseAll(owner + " " + state);
        }
        sequence = 0;
        endOfLevel = false;
        if (phase == Phase.DRAIN) phase = Phase.COLLECT;
    }

    /**
     * Drops what the current level holds (candidates and survivors) but keeps the visited keys. For a repeat that is
     * reset while the visited set must live on.
     */
    public void abandonLevel() {
        dropLevel();
    }

    /**
     * Forgets everything seen.
     */
    public void clear() {
        dropLevel();
        for (final List<SpillRun> runs : visited) {
            for (final SpillRun run : runs) run.discard(ctx.scratch());
            runs.clear();
        }
        if (bits != null) Arrays.fill(bits, 0L);
        if (seen != null) {
            seen.clear();
            ctx.budget().releaseAll(owner + " seen keys");
        }
        if (keys != null) keys.reset();
        phase = Phase.STREAM;
    }

    public void close() {
        try {
            dropLevel();
            for (final List<SpillRun> runs : visited) {
                for (final SpillRun run : runs) run.discard(ctx.scratch());
                runs.clear();
            }
        } finally {
            try {
                if (keys != null) keys.close();
            } finally {
                keys = null;
                if (spillReserve != null) spillReserve.release();
                ctx.budget().releaseAll(owner + " seen bits");
                ctx.budget().releaseAll(owner + " seen keys");
                bits = null;
                seen = null;
            }
        }
    }
}
