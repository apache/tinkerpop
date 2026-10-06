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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;

/**
 * Serializes batch entries, with their bulk, to bytes and back. {@code VAL} entries that refer to a registered column
 * keep the reference, since the column ids are stable for the execution; decoded values are encoded.
 */
final class EntryCodec {

    private final KeyCodec codec;
    private final Lane lane;
    private final boolean recordSource;

    EntryCodec(final KeyCodec codec, final Lane lane, final boolean recordSource) {
        if (lane == Lane.SCALAR) throw new IllegalArgumentException("Entries of the SCALAR lane cannot be spilled");
        this.codec = codec;
        this.lane = lane;
        this.recordSource = recordSource;
    }

    void encode(final Batch b, final int i, final long bulk, final Bytes out) {
        out.putLong(bulk);
        switch (lane) {
            case V:
                out.putInt(b.ord[i]);
                break;
            case E:
                out.putInt(b.ord[i]);
                if (recordSource) out.putInt(b.src[i]);
                break;
            case VP:
                out.putInt(b.ord[i]);
                out.putInt(b.key[i]);
                out.putLong(b.aux[i]);
                break;
            case EP:
                out.putInt(b.ord[i]);
                out.putInt(b.key[i]);
                break;
            case MP:
                out.putInt(b.ord[i]);
                out.putInt(b.key[i]);
                out.putLong(b.aux[i]);
                out.putInt(b.src[i]);
                break;
            default:
                out.putInt(b.key[i]);
                out.putLong(b.aux[i]);
                if (b.key[i] == Batch.DECODED) codec.encodeObject(b.val[i], out);
                break;
        }
    }

    /**
     * Appends the next entry to the batch, which must have room.
     */
    void decode(final ByteSource in, final Batch out) {
        final int n = out.n;
        out.bulk[n] = in.getLong();
        switch (lane) {
            case V:
                out.ord[n] = in.getInt();
                break;
            case E: {
                out.ord[n] = in.getInt();
                final int src = recordSource ? in.getInt() : 0;
                if (out.recordSource) out.src[n] = src;
                break;
            }
            case VP:
                out.ord[n] = in.getInt();
                out.key[n] = in.getInt();
                out.aux[n] = in.getLong();
                break;
            case EP:
                out.ord[n] = in.getInt();
                out.key[n] = in.getInt();
                break;
            case MP:
                out.ord[n] = in.getInt();
                out.key[n] = in.getInt();
                out.aux[n] = in.getLong();
                out.src[n] = in.getInt();
                break;
            default:
                out.key[n] = in.getInt();
                out.aux[n] = in.getLong();
                out.val[n] = out.key[n] == Batch.DECODED ? codec.decodeObject(in) : null;
                break;
        }
        out.n = n + 1;
    }
}
