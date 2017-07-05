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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded;

import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.shaded.kryo.io.Input;

public class ShadedInputAdapter implements InputShim {

    private final Input shadedInput;

    public ShadedInputAdapter(final Input shadedInput) {
        this.shadedInput = shadedInput;
    }

    Input getShadedInput() {
        return shadedInput;
    }

    @Override
    public short readShort() {
        return shadedInput.readShort();
    }

    @Override
    public byte readByte() {
        return shadedInput.readByte();
    }

    @Override
    public byte[] readBytes(final int size) {
        return shadedInput.readBytes(size);
    }

    @Override
    public String readString() {
        return shadedInput.readString();
    }

    @Override
    public long readLong() {
        return shadedInput.readLong();
    }

    @Override
    public int readInt() {
        return shadedInput.readInt();
    }

    @Override
    public double readDouble() {
        return shadedInput.readDouble();
    }
}
