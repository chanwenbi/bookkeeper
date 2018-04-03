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
package org.apache.bookkeeper.proto;

import static com.google.common.base.Charsets.UTF_8;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookieProtoEncoding.EnDecoder;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3;
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
 * Benchmarking serialization and deserilization.
 */
public class ProtocolBenchmark {

    /**
     * EnDecoder type.
     */
    public enum EnDecoderType {
        V3,
        V3_UNSAFE,
        V3_NEW
    }

    @State(Scope.Thread)
    public static class BenchmarkState {

        @Param
        EnDecoderType endecoderType;
        @Param({"100000"})
        int size;

        byte[] masterKey;
        byte[] entryData;
        ByteBuf entryBuf;

        long ledgerId;
        long entryId;

        byte[] requestData;
        ByteBuf heapRequestBuf;

        byte[] responseData;
        ByteBuf heapResponseBuf;

        EnDecoder reqv3EnDecoder;
        EnDecoder respv3EnDecoder;
        EnDecoder reqv3NewEnDecoder;
        EnDecoder respv3NewEnDecoder;

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

            this.responseData = Response.newBuilder()
                .setStatus(StatusCode.EOK)
                .setHeader(BKPacketHeader.newBuilder()
                    .setOperation(OperationType.READ_ENTRY)
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

            this.reqv3EnDecoder = new BookieProtoEncoding.RequestEnDecoderV3(null);
            this.respv3EnDecoder = new BookieProtoEncoding.ResponseEnDecoderV3(null);
            this.reqv3NewEnDecoder = new BookieProtocolV3.RequestEnDecoderV3(null);
            this.respv3NewEnDecoder = new BookieProtocolV3.ResponseEnDecoderV3(null);
        }

        Object constructResponse() {
            switch (endecoderType) {
                case V3_NEW:
                    return constructNewV3Response();
                case V3_UNSAFE:
                    return constructV3ResponseUnsafe();
                default:
                    return constructV3ResponseSafe();
            }
        }

        Object constructNewV3Response() {
            ResponseV3Builder respV3Builder = ResponseV3Builder.get();

            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

            ReadResponse.Builder readBuilder = ReadResponse.newBuilder()
                    .setStatus(StatusCode.EOK)
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setBody(UnsafeByteOperations.unsafeWrap(entryData));

            respV3Builder.setMsgBuilder(
                Response.newBuilder()
                    .setStatus(StatusCode.EOK)
                    .setHeader(headerBuilder)
                    .setReadResponse(readBuilder));
            respV3Builder.retainBuf(entryBuf.retain());

            return respV3Builder;
        }

        Object constructV3ResponseSafe() {
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

            ReadResponse.Builder readBuilder = ReadResponse.newBuilder()
                    .setStatus(StatusCode.EOK)
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    // unsafe wrap buffer
                    .setBody(ByteString.copyFrom(entryData));

            return Response.newBuilder()
                .setStatus(StatusCode.EOK)
                .setHeader(headerBuilder)
                .setReadResponse(readBuilder)
                .build();
        }

        Object constructV3ResponseUnsafe() {
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

            ReadResponse.Builder readBuilder = ReadResponse.newBuilder()
                    .setStatus(StatusCode.EOK)
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    // unsafe wrap buffer
                    .setBody(UnsafeByteOperations.unsafeWrap(entryData));

            return Response.newBuilder()
                .setStatus(StatusCode.EOK)
                .setHeader(headerBuilder)
                .setReadResponse(readBuilder)
                .build();
        }

        EnDecoder getResponseEnDeCoder() {
            switch (endecoderType) {
                case V3_NEW:
                    return respv3NewEnDecoder;
                default:
                    return respv3EnDecoder;
            }
        }

        ByteBuf getResponseBuf() {
            switch (endecoderType) {
                case V3_NEW:
                    return heapResponseBuf.retainedSlice();
                default:
                    return heapResponseBuf.slice();
            }
        }

    }


    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testEncodeResponse(BenchmarkState state) throws Exception {
        Object response = state.constructResponse();
        Object serializedResponse = state.getResponseEnDeCoder().encode(response, PooledByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(serializedResponse);
    }

    @Benchmark
    @BenchmarkMode({ Mode.Throughput })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testDecodeResponse(BenchmarkState state) throws Exception {
        Object deserializedResponse = state.getResponseEnDeCoder().decode(state.getResponseBuf());
        ReferenceCountUtil.release(deserializedResponse);
    }

}
