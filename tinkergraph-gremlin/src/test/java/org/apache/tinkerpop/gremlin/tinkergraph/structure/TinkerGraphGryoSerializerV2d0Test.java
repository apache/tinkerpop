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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TinkerIoRegistry.TinkerGraphGryoSerializer}
 */
@RunWith(MockitoJUnitRunner.class)
public class TinkerGraphGryoSerializerV2d0Test {

    @Mock
    private Kryo kryo;
    @Mock
    private Registration registration;
    @Mock
    private Output output;
    @Mock
    private Input input;

    private TinkerGraph graph = TinkerGraph.open();
    private TinkerIoRegistryV2d0.TinkerGraphGryoSerializer serializer = new TinkerIoRegistryV2d0.TinkerGraphGryoSerializer();

    @Before
    public void setUp() throws Exception {
        when(kryo.getRegistration((Class) any())).thenReturn(registration);
        when(input.readBytes(anyInt())).thenReturn(Arrays.copyOf(GryoMapper.HEADER, 100));
    }

    @Test
    public void shouldVerifyKryoUsedForWrite() throws Exception {
        serializer.write(kryo, output, graph);
        verify(kryo, atLeastOnce()).getRegistration((Class) any());
    }

    @Test
    public void shouldVerifyKryoUsedForRead() throws Exception {
        // Not possible to mock an entire deserialization so just verify the same kryo instances are being used
        try {
            serializer.read(kryo, input, TinkerGraph.class);
        } catch (RuntimeException ex) {
            verify(kryo, atLeastOnce()).readObject(any(), any());
            verify(kryo, atLeastOnce()).readClassAndObject(any());
        }
    }
}
