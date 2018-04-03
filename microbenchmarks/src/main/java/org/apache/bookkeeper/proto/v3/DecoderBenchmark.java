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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.RequestV3;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.ResponseV3;
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
public class DecoderBenchmark {

    /**
     * Test type.
     */
    public enum TestType {
        HEAP,
        DIRECT
    }

    @State(Scope.Thread)
    public static class BenchmarkState {

        @Param
        TestType type;
        @Param({ "100000" })
        int size;
        @Param({ "true", "false" })
        boolean enableAliasing;

        byte[] requestData;
        ByteBuf heapRequestBuf;
        ByteBuf directRequestBuf;

        byte[] responseData;
        ByteBuf heapResponseBuf;
        ByteBuf directResponseBuf;

        @Setup(Level.Trial)
        public void prepare() {
            byte[] masterKey = "test-benchmark-key".getBytes(UTF_8);
            Random r = new Random(System.currentTimeMillis());
            byte[] entryData = new byte[this.size];
            r.nextBytes(entryData);
            long ledgerId = r.nextLong();
            long entryId = r.nextLong();

            this.requestData = Request.newBuilder()
                .setHeader(BKPacketHeader.newBuilder()
                    .setOperation(OperationType.ADD_ENTRY)
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setTxnId(0L)
                    .build())
                .setAddRequest(AddRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(ByteString.copyFrom(masterKey))
                    .setBody(ByteString.copyFrom(entryData))
                    .setFlag(Flag.RECOVERY_ADD)
                    .build())
                .build()
                .toByteArray();
            this.heapRequestBuf = Unpooled.wrappedBuffer(requestData);
            this.directRequestBuf = PooledByteBufAllocator.DEFAULT.directBuffer(requestData.length, requestData.length);
            this.directRequestBuf.writeBytes(requestData);

            this.responseData = Response.newBuilder()
                .setStatus(StatusCode.EOK)
                .setHeader(BKPacketHeader.newBuilder()
                    .setOperation(OperationType.ADD_ENTRY)
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setTxnId(0L)
                    .build())
                .setReadResponse(ReadResponse.newBuilder()
                    .setStatus(StatusCode.EOK)
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setBody(ByteString.copyFrom(entryData))
                    .build())
                .build()
                .toByteArray();
            this.heapResponseBuf = Unpooled.wrappedBuffer(responseData);
            this.directResponseBuf =
                PooledByteBufAllocator.DEFAULT.directBuffer(responseData.length, responseData.length);
            this.directResponseBuf.writeBytes(responseData);
        }

        ByteBuf getRequestBuf() {
            switch (type) {
                case HEAP:
                    return heapRequestBuf.retainedSlice();
                default:
                    return directRequestBuf.retainedSlice();
            }
        }

        ByteBuf getResponseBuf() {
            switch (type) {
                case HEAP:
                    return heapResponseBuf.retainedSlice();
                default:
                    return directResponseBuf.retainedSlice();
            }
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void serializeRequest(BenchmarkState state) throws Exception {
        RequestV3 requestV3 = BookieProtocolV3.parseRequest(
            state.getRequestBuf(),
            null,
            state.enableAliasing);
        ReferenceCountUtil.release(requestV3);
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void serializeResponse(BenchmarkState state) throws Exception {
        ResponseV3 responseV3 = BookieProtocolV3.parseResponse(
            state.getResponseBuf(),
            null,
            state.enableAliasing);
        ReferenceCountUtil.release(responseV3);
    }

}
