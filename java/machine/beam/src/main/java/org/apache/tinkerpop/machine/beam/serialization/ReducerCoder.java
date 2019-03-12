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
package org.apache.tinkerpop.machine.beam.serialization;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.tinkerpop.machine.beam.sideEffect.BasicReducer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReducerCoder<C, S, E> extends Coder<BasicReducer<C, S, E>> {

    @Override
    public void encode(final BasicReducer<C, S, E> value, final OutputStream outStream) throws CoderException, IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(outStream);
        outputStream.writeObject(value);
    }

    @Override
    public BasicReducer<C, S, E> decode(InputStream inStream) throws CoderException, IOException {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(inStream);
            return (BasicReducer<C, S, E>) inputStream.readObject();
        } catch (final ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
