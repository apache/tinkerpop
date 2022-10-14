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
package org.apache.tinkerpop.gremlin.driver;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker13;

import java.util.concurrent.atomic.AtomicInteger;

public class DelayedHandshakeHandler extends WebSocketServerHandshaker13 {

    static AtomicInteger count;

    static {
        count = new AtomicInteger(0);
    }

    public DelayedHandshakeHandler(String webSocketURL, String subprotocols, boolean allowExtensions, int maxFramePayloadLength) {
        super(webSocketURL, subprotocols, allowExtensions, maxFramePayloadLength);
    }

    @Override
    protected FullHttpResponse newHandshakeResponse(FullHttpRequest req, HttpHeaders headers) {
        int curCount = count.incrementAndGet();

        try {
            if (curCount > 2 && curCount < 5) {
                Thread.sleep(2000);
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        return super.newHandshakeResponse(req, headers);
    }
}
