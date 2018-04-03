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
package org.apache.bookkeeper.proto.v3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedStreamUtil;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookieProtoEncoding.EnDecoder;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.RequestEnDecoderV3;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.RequestV3;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.RequestV3Builder;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link RequestEnDecoderV3}.
 */
@Slf4j
public class RequestEnDecoderV3Test {

    @Before
    public void setup() {
        log.info("Has unsafe byte buffer operations {}", CodedStreamUtil.hasUnsafeByteBufferOperations());
    }

    @Test
    public void testAddEntry() throws Exception {
        ByteBuf entry = Unpooled.wrappedBuffer("test-add-request".getBytes(UTF_8));
        AddRequest addRequest = AddRequest.newBuilder()
            .setLedgerId(3456L)
            .setEntryId(5676L)
            .setMasterKey(ByteString.copyFrom("test-master-key", UTF_8))
            .setBody(ByteString.copyFrom(entry.nioBuffer()))
            .build();
        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.ADD_ENTRY)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setAddRequest(addRequest)
            .build();

        RequestV3Builder v3Builder = RequestV3Builder.get();
        v3Builder.setMsgBuilder(Request.newBuilder()
            .setHeader(request.getHeader())
            .setAddRequest(AddRequest.newBuilder(addRequest)
                .clearBody()
                .setBody(UnsafeByteOperations.unsafeWrap(entry.nioBuffer()))));
        v3Builder.retainBuf(entry);

        testEncodeDecodeRequest(v3Builder, entry, request);
    }

    @Test
    public void testReadEntry() throws Exception {
        ReadRequest readRequest = ReadRequest.newBuilder()
            .setLedgerId(3456L)
            .setEntryId(5676L)
            .setMasterKey(ByteString.copyFrom("test-master-key", UTF_8))
            .build();
        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.READ_ENTRY)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setReadRequest(readRequest)
            .build();

        RequestV3Builder v3Builder = RequestV3Builder.get();
        v3Builder.setMsgBuilder(Request.newBuilder()
            .setHeader(request.getHeader())
            .setReadRequest(ReadRequest.newBuilder(readRequest)));

        testEncodeDecodeRequest(v3Builder, null, request);
    }

    @Test
    public void testGetBookieInfo() throws Exception {
        GetBookieInfoRequest getRequest = GetBookieInfoRequest.newBuilder()
            .build();
        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.GET_BOOKIE_INFO)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setGetBookieInfoRequest(getRequest)
            .build();

        RequestV3Builder v3Builder = RequestV3Builder.get();
        v3Builder.setMsgBuilder(Request.newBuilder()
            .setHeader(request.getHeader())
            .setGetBookieInfoRequest(GetBookieInfoRequest.newBuilder(getRequest)));

        testEncodeDecodeRequest(v3Builder, null, request);
    }

    @Test
    public void testWriteLac() throws Exception {
        ByteBuf entry = Unpooled.wrappedBuffer("test-write-lac-request".getBytes(UTF_8));
        WriteLacRequest writeLacRequest = WriteLacRequest.newBuilder()
            .setLedgerId(3456L)
            .setLac(12345L)
            .setMasterKey(ByteString.copyFrom("test-master-key", UTF_8))
            .setBody(ByteString.copyFrom(entry.nioBuffer()))
            .build();

        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.WRITE_LAC)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setWriteLacRequest(writeLacRequest)
            .build();

        RequestV3Builder v3Builder = RequestV3Builder.get();
        v3Builder.setMsgBuilder(Request.newBuilder()
            .setHeader(request.getHeader())
            .setWriteLacRequest(WriteLacRequest.newBuilder(writeLacRequest)
                .clearBody()
                .setBody(UnsafeByteOperations.unsafeWrap(entry.nioBuffer()))));
        v3Builder.retainBuf(entry);

        testEncodeDecodeRequest(v3Builder, entry, request);
    }

    @Test
    public void testReadLac() throws Exception {
        ReadLacRequest readRequest = ReadLacRequest.newBuilder()
            .setLedgerId(3456L)
            .build();
        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.READ_LAC)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setReadLacRequest(readRequest)
            .build();

        RequestV3Builder v3Builder = RequestV3Builder.get();
        v3Builder.setMsgBuilder(Request.newBuilder()
            .setHeader(request.getHeader())
            .setReadLacRequest(ReadLacRequest.newBuilder(readRequest)));

        testEncodeDecodeRequest(v3Builder, null, request);
    }

    private void testEncodeDecodeRequest(RequestV3Builder request,
                                         ByteBuf retainedBuf,
                                         Request expectedRequest) throws Exception {
        EnDecoder coder = new RequestEnDecoderV3(null);

        Object serialized = coder.encode(request, PooledByteBufAllocator.DEFAULT);
        assertTrue(serialized instanceof ByteBufList);
        ByteBufList serializedBufList = (ByteBufList) serialized;

        // request is recycled but `retainedBuf` is still alive because `serializedBufList` keeps reference
        assertEquals(0, request.refCnt());
        assertEquals(1, serializedBufList.refCnt());
        if (null != retainedBuf) {
            assertEquals(1, retainedBuf.refCnt());
        }

        // transfer data from `serializedBufList` to a buffer
        int bufferSize = serializedBufList.readableBytes();
        byte[] data = new byte[bufferSize];
        assertEquals(bufferSize, serializedBufList.getBytes(data));

        // serialized buf list is released which also release `retainedBuf`
        serializedBufList.release();
        assertEquals(0, serializedBufList.refCnt());
        if (null != retainedBuf) {
            assertEquals(0, retainedBuf.refCnt());
        }

        byte[] expectedData = expectedRequest.toByteArray();
        assertEquals(expectedData.length, data.length);
        assertArrayEquals(
            "Expected data : " + Hex.encodeHexString(expectedData) + "\n"
                    + "Actual data   : " + Hex.encodeHexString(data) + "\n",
            expectedData, data);

        // prepare the buffer for decoding
        ByteBuf packet = Unpooled.wrappedBuffer(data);
        Object deserialized = coder.decode(packet);
        assertTrue(deserialized instanceof RequestV3);

        RequestV3 reqv3 = (RequestV3) deserialized;
        // `reqv3` is referencing `packet`. so `packet` refcnt is not 0.
        assertEquals(1, reqv3.refCnt());
        assertEquals(expectedRequest, reqv3.getUnderlyingRequest());

        // if it is add request, make sure we don't copy data to entry buffer
        if (OperationType.ADD_ENTRY == reqv3.getOpType()
            || OperationType.WRITE_LAC == reqv3.getOpType()) {
            // retained the add entry buffer
            ByteBuf entryBuffer;
            if (OperationType.ADD_ENTRY == reqv3.getOpType()) {
                entryBuffer = reqv3.retainedEntryFromAddRequest();
            } else {
                entryBuffer = reqv3.retainedLacBufFromWriteLacRequest();
            }
            // packet refcnt will be incremented by 1
            assertEquals(2, packet.refCnt());
            assertEquals(1, entryBuffer.refCnt());

            // release the request will decrement refcnt by 1
            reqv3.release();
            assertEquals(1, packet.refCnt());
            assertEquals(1, entryBuffer.refCnt());

            // release entry buffer will eventually release underlying packet buffer
            entryBuffer.release();
            assertEquals(0, packet.refCnt());
            assertEquals(0, entryBuffer.refCnt());
        }
    }

}
