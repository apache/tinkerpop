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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.util.Arrays;
import java.util.zip.Deflater;

/**
 * Handles compression of content when the response contains the "Content-Encoding" header. Currently supports "deflate".
 * This class is specifically meant to work with serialized GraphBinary chunks as they need to be deflated per chunk such
 * that the chunk ends with a compressed Marker.END_OF_STREAM.
 */
@ChannelHandler.Sharable
public class HttpContentCompressionHandler extends ChannelOutboundHandlerAdapter {
    private static final AttributeKey<Deflater> DEFLATER = AttributeKey.valueOf("deflater");
    private static final int MIN_BUFFER_SIZE = 16; // Don't want a buffer too small as it could cause lots of resizing.

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        Object out = msg;
        int writeIdx = 0;
        int bytesWritten = 0;

        if (msg instanceof HttpResponse && (((HttpResponse) msg).headers().contains(HttpHeaderNames.CONTENT_ENCODING))) {
            ctx.attr(DEFLATER).set(new Deflater());
        }

        if (msg instanceof HttpContent) {
            final ByteBuf chunk = ((HttpContent) msg).content();
            final Deflater compressor = ctx.attr(DEFLATER).get();
            if (null == compressor) {
                super.write(ctx, msg, promise);
                return;
            }

            compressor.setInput(ByteBufUtil.getBytes(chunk)); // need to copy bytes as it may be in a direct buffer.
            if (msg instanceof LastHttpContent) { compressor.finish(); }

            final int compressedSizeEstimate = chunk.readableBytes() / 8;
            byte[] outBuf = new byte[Math.max(compressedSizeEstimate, MIN_BUFFER_SIZE)];
            // Need to SYNC_FLUSH to ensure that each chunk's data is completely compressed.
            bytesWritten = compressor.deflate(outBuf, writeIdx, outBuf.length, Deflater.SYNC_FLUSH);

            while (bytesWritten == (outBuf.length - writeIdx)) {
                writeIdx += bytesWritten;
                outBuf = Arrays.copyOf(outBuf, outBuf.length * 2);
                bytesWritten = compressor.deflate(outBuf, writeIdx, outBuf.length - writeIdx, Deflater.SYNC_FLUSH);
            }

            writeIdx += bytesWritten;
            out = ((HttpContent) msg).replace(Unpooled.wrappedBuffer(outBuf).writerIndex(writeIdx));
            ReferenceCountUtil.release(msg);

            if (out instanceof LastHttpContent) { ctx.attr(DEFLATER).getAndSet(null).end(); }
        }

        super.write(ctx, out, promise);
    }
}
