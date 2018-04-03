/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto.v3;

import static com.google.common.base.Charsets.UTF_8;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.PBMessageBuilder;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.RequestV3Builder;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.ResponseV3Builder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarking serialization and deserialization.
 */
public class EncoderBenchmark {

    /**
     * Test type.
     */
    public enum TestType {
        ARRAY_TO_ARRAY,
        ARRAY_TO_DIRECT,
        DIRECT_TO_DIRECT,
        ARRAY_LAZY,
        DIRECT_LAZY
    }

    @State(Scope.Thread)
    public static class BenchmarkState {

        @Param
        TestType type;
        @Param({ "100000" })
        int size;

        byte[] masterKey;
        byte[] entryData;
        ByteBuf entryBuf;

        long ledgerId;
        long entryId;
        short flags;

        @Setup(Level.Trial)
        public void prepare() {
            this.masterKey = "test-benchmark-key".getBytes(UTF_8);
            Random r = new Random(System.currentTimeMillis());
            this.entryData = new byte[this.size];
            r.nextBytes(entryData);
            this.entryBuf = PooledByteBufAllocator.DEFAULT.directBuffer(size, size);
            this.entryBuf.writeBytes(entryData);
            this.ledgerId = r.nextLong();
            this.entryId = r.nextLong();
            this.flags = 1;
        }

        ByteString getEntryData() {
            switch (type) {
                case ARRAY_LAZY:
                case ARRAY_TO_ARRAY:
                case ARRAY_TO_DIRECT:
                    return UnsafeByteOperations.unsafeWrap(entryData);
                default:
                    return UnsafeByteOperations.unsafeWrap(entryBuf.nioBuffer());
            }
        }

        Object serialize(PBMessageBuilder builder, ByteBufAllocator allocator) {
            switch (type) {
                case ARRAY_TO_ARRAY:
                    return builder.serializeHeap(allocator);
                case DIRECT_TO_DIRECT:
                case ARRAY_TO_DIRECT:
                    return builder.serializeDirect(allocator);
                default:
                    return builder.serializeLazy(allocator);
            }
        }

    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void serializeResponse(BenchmarkState state) {
        ResponseV3Builder respV3Builder = ResponseV3Builder.get();

        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

        ReadResponse.Builder readBuilder = ReadResponse.newBuilder()
                .setStatus(StatusCode.EOK)
                .setLedgerId(state.ledgerId)
                .setEntryId(state.entryId)
                .setBody(state.getEntryData());

        respV3Builder.setMsgBuilder(
            Response.newBuilder()
                .setStatus(StatusCode.EOK)
                .setHeader(headerBuilder)
                .setReadResponse(readBuilder));
        respV3Builder.retainBuf(state.entryBuf.retain());

        Object res = state.serialize(respV3Builder, PooledByteBufAllocator.DEFAULT);
        respV3Builder.release();
        ReferenceCountUtil.safeRelease(res);
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void serializeRequest(BenchmarkState state) {
        RequestV3Builder reqv3Builder = RequestV3Builder.get();

        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(state.ledgerId)
                .setEntryId(state.entryId)
                .setMasterKey(UnsafeByteOperations.unsafeWrap(state.masterKey))
                .setBody(state.getEntryData())
                .setFlag(AddRequest.Flag.RECOVERY_ADD);

        reqv3Builder.setMsgBuilder(
            Request.newBuilder()
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder));
        reqv3Builder.retainBuf(state.entryBuf.retain());

        Object res = state.serialize(reqv3Builder, PooledByteBufAllocator.DEFAULT);
        reqv3Builder.release();
        ReferenceCountUtil.safeRelease(res);
    }

}
