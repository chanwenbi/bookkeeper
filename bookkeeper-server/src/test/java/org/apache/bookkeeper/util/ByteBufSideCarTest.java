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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

/**
 * Unit tests for {@link ByteBufSideCar}.
 */
public class ByteBufSideCarTest {

    @Test
    public void testWithoutSidecar() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBufSideCar buf = ByteBufSideCar.get(b1);

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());
        assertEquals(128, buf.readableBytes());

        assertSame(b1.array(), buf.array());
        assertEquals(b1.arrayOffset(), buf.arrayOffset());
        assertEquals(b1.readableBytes(), buf.readableBytes());

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
    }

    @Test
    public void testWithSidecar() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(64, 64);
        b2.writerIndex(b2.capacity());

        ByteBufSideCar buf = ByteBufSideCar.get(b1);
        buf.addToSideCar(b2);

        assertEquals(1, buf.refCnt());
        assertEquals(1, b1.refCnt());
        assertEquals(1, b2.refCnt());
        assertEquals(128, buf.readableBytes());

        assertSame(b1.array(), buf.array());
        assertEquals(b1.arrayOffset(), buf.arrayOffset());
        assertEquals(b1.readableBytes(), buf.readableBytes());

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    /**
     * The `ByteBufSideCar` itself is a recyclable object.
     */
    @Test
    public void testRecycle() {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b1.capacity());

        ByteBufSideCar car1 = ByteBufSideCar.get(b1);
        car1.release();
        ByteBufSideCar car2 = ByteBufSideCar.get(b2);

        assertSame(car1, car2);
        car2.release();
    }

}
