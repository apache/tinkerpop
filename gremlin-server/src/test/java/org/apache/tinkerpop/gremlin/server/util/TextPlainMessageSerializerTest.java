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
package org.apache.tinkerpop.gremlin.server.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TextPlainMessageSerializerTest {
    @Test
    public void shouldProducePlainText() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        final ResponseMessage msg = ResponseMessage.build().
                code(HttpResponseStatus.OK).
                result(Arrays.asList(1, new DetachedVertex(100, "person", m), java.awt.Color.RED)).create();

        final TextPlainMessageSerializer messageSerializer = new TextPlainMessageSerializer();
        final ByteBuf output = messageSerializer.serializeResponseAsBinary(msg, ByteBufAllocator.DEFAULT);
        final String exp = "==>1" + System.lineSeparator() +
                "==>v[100]" + System.lineSeparator() +
                "==>java.awt.Color[r=255,g=0,b=0]";
        assertEquals(exp, output.toString(CharsetUtil.UTF_8));
    }
}
