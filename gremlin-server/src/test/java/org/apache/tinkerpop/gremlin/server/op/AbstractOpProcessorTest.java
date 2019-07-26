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
package org.apache.tinkerpop.gremlin.server.op;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractOpProcessorTest {

    @Test
    public void makeFrameMethodTest() throws Exception {
        final RequestMessage request = RequestMessage.build("test").create();
        final Context ctx = Mockito.mock(Context.class);

        final ArgumentCaptor<ResponseMessage> responseCaptor = ArgumentCaptor.forClass(ResponseMessage.class);

        try {
            // Induce a NullPointerException to validate error response message writing
            AbstractOpProcessor.makeFrame(ctx, request, null, true, null, ResponseStatusCode.PARTIAL_CONTENT, null, null);
            fail("Expected a NullPointerException");
        } catch (NullPointerException expected) {
            // nop
        }

        Mockito.verify(ctx, Mockito.times(1)).writeAndFlush(responseCaptor.capture());
        assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, responseCaptor.getValue().getStatus().getCode());
        assertEquals(request.getRequestId(), responseCaptor.getValue().getRequestId());
    }

}