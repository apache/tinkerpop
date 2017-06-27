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

import com.esotericsoftware.kryo.io.Input;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;

public class UnshadedInputAdapter implements InputShim {

    private final Input unshadedInput;

    public UnshadedInputAdapter(final Input unshadedInput) {
        this.unshadedInput = unshadedInput;
    }

    Input getUnshadedInput() {
        return unshadedInput;
    }

    @Override
    public byte readByte() {
        return unshadedInput.readByte();
    }

    @Override
    public byte[] readBytes(final int size) {
        return unshadedInput.readBytes(size);
    }

    @Override
    public String readString() {
        return unshadedInput.readString();
    }

    @Override
    public long readLong() {
        return unshadedInput.readLong();
    }

    @Override
    public int readInt() {
        return unshadedInput.readInt();
    }

    @Override
    public short readShort() {
        return unshadedInput.readShort();
    }

    @Override
    public double readDouble() {
        return unshadedInput.readDouble();
    }

    @Override
    public boolean readBoolean() {
        return unshadedInput.readBoolean();
    }
}
