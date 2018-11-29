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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types.sample;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializerRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

public class SamplePersonSerializerTest {

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV1d0 serializer = new GraphBinaryMessageSerializerV1d0(
            TypeSerializerRegistry.build().addCustomType(SamplePerson.class, new SamplePersonSerializer()).create());

    @Test
    public void customSerializationWithPersonTest() throws SerializationException {
        Date birthDate = Date.from(LocalDateTime.of(2010, 4, 29, 5, 30).toInstant(ZoneOffset.UTC));
        SamplePerson person = new SamplePerson("Olivia", birthDate);

        ByteBuf serialized = serializer.serializeResponseAsBinary(
                ResponseMessage.build(UUID.randomUUID()).result(person).create(), allocator);

        ResponseMessage deserialized = serializer.deserializeResponse(serialized);

        SamplePerson actual = (SamplePerson) deserialized.getResult().getData();
        Assert.assertThat(actual, new ReflectionEquals(person));
    }
}
