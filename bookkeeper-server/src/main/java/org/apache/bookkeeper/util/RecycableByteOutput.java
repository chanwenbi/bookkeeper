/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.util;

import com.google.protobuf.ByteOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A recycable implementation of protobuf {@link ByteOutput}.
 */
public class RecycableByteOutput extends ByteOutput {

    public static RecycableByteOutput create(int bufferSize, ByteBufAllocator allocator) {
        RecycableByteOutput output = RECYCLER.get();
        output.bufferSize = bufferSize;
        output.allocator = allocator;
        output.bufList = ByteBufList.get();
        return output;
    }

    private static final Recycler<RecycableByteOutput> RECYCLER = new Recycler<RecycableByteOutput>() {
        @Override
        protected RecycableByteOutput newObject(Handle<RecycableByteOutput> handle) {
            return new RecycableByteOutput(handle);
        }
    };

    private final Handle<RecycableByteOutput> handle;
    private int bufferSize;
    private ByteBufAllocator allocator;
    ByteBuf buf;
    ByteBufList bufList;

    private RecycableByteOutput(Handle<RecycableByteOutput> handle) {
        this.handle = handle;
    }

    private void ensureBufCreated() {
        if (null == buf) {
            buf = allocator.buffer(bufferSize, bufferSize);
            bufList.add(buf);
        }
    }

    @Override
    public void write(byte value) throws IOException {
        ensureBufCreated();
        buf.writeByte(value);
    }

    @Override
    public void write(byte[] value, int offset, int length) throws IOException {
        buf = null;
        bufList.add(Unpooled.copiedBuffer(value, offset, length));
    }

    @Override
    public void writeLazy(byte[] value, int offset, int length) throws IOException {
        buf = null;
        bufList.add(Unpooled.wrappedBuffer(value, offset, length));
    }

    @Override
    public void write(ByteBuffer value) throws IOException {
        buf = null;
        bufList.add(Unpooled.copiedBuffer(value));
    }

    @Override
    public void writeLazy(ByteBuffer value) throws IOException {
        buf = null;
        bufList.add(Unpooled.wrappedBuffer(value));
    }

    /**
     * Complete the output to get the buffer list and also recycle this instance.
     *
     * @return the buffer list accumulated in this output
     */
    public ByteBufList complete() {
        ByteBufList bufListToRet = bufList;
        bufList = null;
        buf = null;
        return bufListToRet;
    }

    public void recycle() {
        if (null != bufList) {
            ReferenceCountUtil.safeRelease(bufList);
            bufList = null;
            buf = null;
        }
        bufferSize = -1;
        allocator = null;
        handle.recycle(this);
    }
}
