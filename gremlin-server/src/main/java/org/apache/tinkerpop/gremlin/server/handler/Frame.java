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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.util.ReferenceCounted;

/**
 * A holder for a {@code String} or {@code ByteBuf} that represents a message to be written back to the requesting
 * client.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Frame {
    private final Object msg;

    public Frame(final Object msg) {
        this.msg = msg;
    }

    public Object getMsg() {
        return msg;
    }

    /**
     * If the object contained in the frame is {@code ReferenceCounted} then it may need to be released or else
     * Netty will generate warnings that counted resources are leaking.
     */
    public void tryRelease() {
        if (msg instanceof ReferenceCounted)
            ((ReferenceCounted) msg).release();
    }
}
