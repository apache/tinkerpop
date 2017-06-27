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
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded;

import com.esotericsoftware.kryo.io.Output;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;

public class UnshadedOutputAdapter implements OutputShim {
    private final Output unshadedOutput;

    public UnshadedOutputAdapter(final Output unshadedOutput) {
        this.unshadedOutput = unshadedOutput;
    }

    Output getUnshadedOutput() {
        return unshadedOutput;
    }

    @Override
    public void writeByte(final byte b) {
        unshadedOutput.writeByte(b);
    }

    @Override
    public void writeBytes(final byte[] array, final int offset, final int count) {
        unshadedOutput.writeBytes(array, offset, count);
    }

    @Override
    public void writeShort(int s) {
        unshadedOutput.writeShort(s);
    }

    @Override
    public void writeString(final String s) {
        unshadedOutput.writeString(s);
    }

    @Override
    public void writeLong(final long l) {
        unshadedOutput.writeLong(l);
    }

    @Override
    public void writeInt(final int i) {
        unshadedOutput.writeInt(i);
    }

    @Override
    public void writeDouble(final double d) {
        unshadedOutput.writeDouble(d);
    }

    @Override
    public void writeBoolean(final boolean b) {
        unshadedOutput.writeBoolean(b);
    }

    @Override
    public void flush() {
        unshadedOutput.flush();
    }
}
