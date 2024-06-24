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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Handles decompression of content when the response contains the "Content-Encoding" header. Currently supports "deflate".
 * This class is specifically meant to work with serialized GraphBinary chunks as they need to be inflated per chunk such
 * that the chunk ends with a Marker.END_OF_STREAM.
 */
@ChannelHandler.Sharable
public class HttpContentDecompressionHandler extends ChannelInboundHandlerAdapter {
    private static final AttributeKey<Inflater> INFLATER = AttributeKey.valueOf("inflater");
    private static final int MIN_BUFFER_SIZE = 16; // Don't want a buffer too small as it could cause lots of resizing.

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object out = msg;

        if (msg instanceof HttpResponse && (hasDeflateTransferEncoding(((HttpResponse) msg).headers()))) {
            ctx.attr(INFLATER).set(new Inflater());
        }

        if (msg instanceof HttpContent) {
            final Inflater decompressor = ctx.attr(INFLATER).get();
            if (null == decompressor) {
                super.channelRead(ctx, msg);
                return;
            }

            final ByteBuf chunk = ((HttpContent) msg).content();
            decompressor.setInput(ByteBufUtil.getBytes(chunk));
            final int decompressedSizeEstimate = chunk.readableBytes() * 8;
            byte[] outBuf = new byte[Math.max(decompressedSizeEstimate, MIN_BUFFER_SIZE)];

            try {
                int writeIdx = 0;
                int bytesWritten = decompressor.inflate(outBuf, writeIdx, outBuf.length);

                while (bytesWritten == (outBuf.length - writeIdx)) {
                    writeIdx += bytesWritten;
                    outBuf = Arrays.copyOf(outBuf, outBuf.length * 2);
                    bytesWritten = decompressor.inflate(outBuf, writeIdx, outBuf.length - writeIdx);
                }

                writeIdx += bytesWritten;
                out = ((HttpContent) msg).replace(Unpooled.wrappedBuffer(outBuf).writerIndex(writeIdx));
                ReferenceCountUtil.release(msg);

                if (out instanceof LastHttpContent) { ctx.attr(INFLATER).getAndSet(null).end(); }
            } catch (DataFormatException dfe) {
                ctx.attr(INFLATER).getAndSet(null).end();
                ctx.fireExceptionCaught(dfe);
                return;
            }
        }

        super.channelRead(ctx, out);
    }

    private boolean hasDeflateTransferEncoding(HttpHeaders headers) {
        for (String value : headers.getAll(HttpHeaderNames.CONTENT_ENCODING)) {
            if ("deflate".equals(value)) {
                return true;
            }
        }

        return false;
    }
}
