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

import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.script.SimpleBindings;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;

public class AbstractEvalOpProcessorTest {

    @Test
    public void evalOpInternalShouldHandleAllEvaluationExceptions() throws OpProcessorException {
        final AbstractEvalOpProcessor processor = new StandardOpProcessor();
        final RequestMessage request = RequestMessage.build("test").
                addArg(Tokens.ARGS_LANGUAGE, "gremlin-groovy").
                addArg(Tokens.ARGS_GREMLIN, "1+1").create();
        final Settings settings = new Settings();
        final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        final ArgumentCaptor<ResponseMessage> responseCaptor = ArgumentCaptor.forClass(ResponseMessage.class);

        final GremlinExecutor gremlinExecutor = Mockito.mock(GremlinExecutor.class);
        final CompletableFuture<Object> exFut = new CompletableFuture<>();
        Mockito.when(gremlinExecutor.eval(anyString(), anyString(), Mockito.any(), Mockito.<GremlinExecutor.LifeCycle>any()))
                .thenReturn(exFut);
        exFut.completeExceptionally(new IllegalStateException("test-exception"));

        final Context contextspy = Mockito.spy(new Context(request, ctx, settings, null, gremlinExecutor, null));

        processor.evalOpInternal(contextspy, contextspy::getGremlinExecutor, SimpleBindings::new);

        Mockito.verify(contextspy, Mockito.times(1)).writeAndFlush(responseCaptor.capture());
        assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, responseCaptor.getValue().getStatus().getCode());
        assertEquals(request.getRequestId(), responseCaptor.getValue().getRequestId());
        assertThat(responseCaptor.getValue().getStatus().getMessage(), CoreMatchers.containsString("test-exception"));
    }
}