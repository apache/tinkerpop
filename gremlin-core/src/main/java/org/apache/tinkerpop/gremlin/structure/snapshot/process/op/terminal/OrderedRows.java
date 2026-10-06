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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The sort keys and rows shared by {@code TopK} and {@code Sort}: a {@link Row} holds one input entry with its
 * evaluated sort keys and arrival sequence, and {@link #order()} compares rows by the keys with
 * {@link Order#asc}/{@link Order#desc}, which order by {@code GremlinValueComparator.ORDERABILITY}, and breaks ties by
 * arrival so that the sort is stable.
 */
final class OrderedRows implements AutoCloseable {

    /**
     * Bytes reserved for a row apart from its keys: the object, its fields and bookkeeping in a collection.
     */
    private static final long ROW_BYTES = 112;
    private static final long KEY_BYTES = 40;

    /**
     * One kept entry.
     */
    static final class Row {
        int ord;
        int key;
        int src;
        long aux;
        Object val;
        long bulk;
        Object[] keys;
        long seq;
    }

    private final CsrExecutionContext ctx;
    private final List<Order> orders;
    private final List<KeyReader> readers = new ArrayList<>();
    private final Comparator<Row> order;
    private long sequence;

    OrderedRows(final CsrExecutionContext ctx, final List<Keys.Key> keys, final List<Order> orders, final Lane lane) {
        this.ctx = ctx;
        this.orders = orders;
        for (final Order o : orders) {
            if (o != Order.asc && o != Order.desc) {
                throw new IllegalArgumentException("Only Order.asc and Order.desc compile natively, not " + o);
            }
        }
        for (final Keys.Key key : keys) readers.add(new KeyReader(ctx, key, lane));
        this.order = (a, b) -> {
            for (int k = 0; k < a.keys.length; k++) {
                final int c = orders.get(k).compare(a.keys[k], b.keys[k]);
                if (c != 0) return c;
            }
            return Long.compare(a.seq, b.seq);
        };
    }

    Comparator<Row> order() {
        return order;
    }

    long rowBytes() {
        return ROW_BYTES + KEY_BYTES * readers.size();
    }

    /**
     * The row of entry {@code i}, or null when a sort key is non-productive, which filters the entry.
     */
    Row row(final Batch b, final int i) {
        final Object[] keys = new Object[readers.size()];
        for (int k = 0; k < keys.length; k++) {
            final Object key = readers.get(k).read(b, i);
            if (key == KeyReader.UNPRODUCTIVE) return null;
            keys[k] = key;
        }
        final Row row = new Row();
        if (b.ord != null) row.ord = b.ord[i];
        if (b.key != null) row.key = b.key[i];
        if (b.aux != null) row.aux = b.aux[i];
        if (b.src != null) row.src = b.src[i];
        if (b.val != null) row.val = b.val[i];
        row.bulk = b.bulk[i];
        row.keys = keys;
        row.seq = sequence++;
        return row;
    }

    /**
     * Appends the row, with the given bulk, to the batch.
     */
    static void emit(final Batch out, final Row row, final long bulk) {
        if (out.ord != null) out.ord[out.n] = row.ord;
        if (out.key != null) out.key[out.n] = row.key;
        if (out.aux != null) out.aux[out.n] = row.aux;
        if (out.src != null) out.src[out.n] = row.src;
        if (out.val != null) out.val[out.n] = row.val;
        out.bulk[out.n++] = bulk;
    }

    void reset() {
        sequence = 0;
        for (final KeyReader reader : readers) reader.reset();
    }

    @Override
    public void close() {
        for (final KeyReader reader : readers) reader.close();
        readers.clear();
    }
}
