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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.FileRegion;
import io.netty.channel.MessageSizeEstimator;

import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * A {@code MessageSizeEstimator} that sizes the {@link Frame} class. This is basically an implementation of netty's
 * default but with that additional feature.
 */
public final class FrameMessageSizeEstimator implements MessageSizeEstimator {

    private static final class HandleImpl implements Handle {
        private final int unknownSize;

        private HandleImpl(int unknownSize) {
            this.unknownSize = unknownSize;
        }

        @Override
        public int size(Object msg) {
            if (msg instanceof Frame) {
                return size0(((Frame) msg).getMsg());
            } else {
                return size0(msg);
            }
        }

        /**
         * Standard netty implementation.
         */
        private int size0(final Object msg) {
            if (msg instanceof ByteBuf) {
                return ((ByteBuf) msg).readableBytes();
            }
            if (msg instanceof ByteBufHolder) {
                return ((ByteBufHolder) msg).content().readableBytes();
            }
            if (msg instanceof FileRegion) {
                return 0;
            }
            return unknownSize;
        }
    }

    /**
     * Return the default implementation which returns {@code 8} for unknown messages which matches the
     * implementation of netty at {@code DefaultMessageSizeEstimator}
     */
    private static final FrameMessageSizeEstimator instance = new FrameMessageSizeEstimator(8);

    private final Handle handle;

    /**
     * Create a new instance
     *
     * @param unknownSize   The size which is returned for unknown messages.
     */
    public FrameMessageSizeEstimator(int unknownSize) {
        checkPositiveOrZero(unknownSize, "unknownSize");
        handle = new HandleImpl(unknownSize);
    }

    public static FrameMessageSizeEstimator instance() {
        return instance;
    }

    @Override
    public Handle newHandle() {
        return handle;
    }
}