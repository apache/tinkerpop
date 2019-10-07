package org.apache.tinkerpop.gremlin.driver;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

final class NettyBuffer implements Buffer {
    private final ByteBuf buffer;

    /**
     * Creates a new instance.
     * @param buffer The buffer to wrap.
     */
    NettyBuffer(ByteBuf buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer can't be null");
        }

        this.buffer = buffer;
    }

    @Override
    public int readerIndex() {
        return this.buffer.readerIndex();
    }

    @Override
    public Buffer readerIndex(int readerIndex) {
        this.buffer.readerIndex(readerIndex);
        return this;
    }

    @Override
    public int writerIndex() {
        return this.buffer.writerIndex();
    }

    @Override
    public Buffer writerIndex(int writerIndex) {
        this.buffer.writerIndex(writerIndex);
        return this;
    }

    @Override
    public int capacity() {
        return this.buffer.capacity();
    }

    @Override
    public boolean readBoolean() {
        return this.buffer.readBoolean();
    }

    @Override
    public byte readByte() {
        return this.buffer.readByte();
    }

    @Override
    public short readShort() {
        return this.buffer.readShort();
    }

    @Override
    public int readInt() {
        return this.buffer.readInt();
    }

    @Override
    public long readLong() {
        return this.buffer.readLong();
    }

    @Override
    public float readFloat() {
        return this.buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return this.buffer.readDouble();
    }

    @Override
    public Buffer readBytes(byte[] destination) {
        this.buffer.readBytes(destination);
        return this;
    }

    @Override
    public Buffer readBytes(byte[] destination, int dstIndex, int length) {
        this.buffer.readBytes(destination, dstIndex, length);
        return this;
    }

    @Override
    public Buffer readBytes(ByteBuffer dst) {
        this.buffer.readBytes(dst);
        return this;
    }

    @Override
    public Buffer readBytes(OutputStream out, int length) throws IOException {
        this.buffer.readBytes(out, length);
        return this;
    }

    @Override
    public Buffer writeBoolean(boolean value) {
        this.buffer.writeBoolean(value);
        return this;
    }

    @Override
    public Buffer writeByte(int value) {
        this.buffer.writeByte(value);
        return this;
    }

    @Override
    public Buffer writeShort(int value) {
        this.buffer.writeShort(value);
        return this;
    }

    @Override
    public Buffer writeInt(int value) {
        this.buffer.writeInt(value);
        return this;
    }

    @Override
    public Buffer writeLong(long value) {
        this.buffer.writeLong(value);
        return this;
    }

    @Override
    public Buffer writeFloat(float value) {
        this.buffer.writeFloat(value);
        return this;
    }

    @Override
    public Buffer writeDouble(double value) {
        this.buffer.writeDouble(value);
        return this;
    }

    @Override
    public Buffer writeBytes(byte[] src) {
        this.buffer.writeBytes(src);
        return this;
    }

    @Override
    public Buffer writeBytes(byte[] src, int srcIndex, int length) {
        this.buffer.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public boolean release() {
        return this.buffer.release();
    }

    @Override
    public Buffer retain() {
        this.buffer.retain();
        return this;
    }

    @Override
    public int referenceCount() {
        return this.buffer.refCnt();
    }
}
