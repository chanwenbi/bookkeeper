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
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookieProtoEncoding.EnDecoder;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.ResponseEnDecoderV3;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.ResponseV3;
import org.apache.bookkeeper.proto.v3.BookieProtocolV3.ResponseV3Builder;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

/**
 * Unit test {@link ResponseEnDecoderV3}.
 */
@Slf4j
public class ResponseEnDecoderV3Test {

    @Test
    public void testAddEntry() throws Exception {
        AddResponse addResponse = AddResponse.newBuilder()
            .setStatus(StatusCode.EOK)
            .setLedgerId(3456L)
            .setEntryId(5676L)
            .build();
        Response response = Response.newBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.ADD_ENTRY)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setAddResponse(addResponse)
            .build();

        ResponseV3Builder v3Builder = BookieProtocolV3.newResponseBuilder(OperationType.ADD_ENTRY);
        v3Builder.getMsgBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(response.getHeader())
            .setAddResponse(AddResponse.newBuilder(addResponse));

        testEncodeDecodeResponse(v3Builder, null, null, response);
    }

    @Test
    public void testReadEntry() throws Exception {
        ByteBuf entry = Unpooled.wrappedBuffer("test-read-entry-request".getBytes(UTF_8));
        ReadResponse readResponse = ReadResponse.newBuilder()
            .setStatus(StatusCode.EOK)
            .setLedgerId(3456L)
            .setEntryId(5676L)
            .setBody(ByteString.copyFrom(entry.nioBuffer()))
            .build();
        Response response = Response.newBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.READ_ENTRY)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setReadResponse(readResponse)
            .build();

        ResponseV3Builder v3Builder = BookieProtocolV3.newResponseBuilder(OperationType.READ_ENTRY);
        v3Builder.getMsgBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(response.getHeader())
            .setReadResponse(ReadResponse.newBuilder(readResponse)
                .clearBody()
                .setBody(UnsafeByteOperations.unsafeWrap(entry.nioBuffer()))
            );
        v3Builder.retainBuf(entry);

        testEncodeDecodeResponse(v3Builder, entry, null, response);
    }

    @Test
    public void testGetBookieInfo() throws Exception {
        GetBookieInfoResponse getResponse = GetBookieInfoResponse.newBuilder()
            .setStatus(StatusCode.EOK)
            .setFreeDiskSpace(100)
            .setTotalDiskCapacity(200)
            .build();
        Response response = Response.newBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.GET_BOOKIE_INFO)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setGetBookieInfoResponse(getResponse)
            .build();

        ResponseV3Builder v3Builder = BookieProtocolV3.newResponseBuilder(OperationType.GET_BOOKIE_INFO);
        v3Builder.getMsgBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(response.getHeader())
            .setGetBookieInfoResponse(GetBookieInfoResponse.newBuilder(getResponse));

        testEncodeDecodeResponse(v3Builder, null, null, response);
    }

    @Test
    public void testWriteLac() throws Exception {
        WriteLacResponse writeLacResponse = WriteLacResponse.newBuilder()
            .setStatus(StatusCode.EOK)
            .setLedgerId(3456L)
            .build();

        Response response = Response.newBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.WRITE_LAC)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setWriteLacResponse(writeLacResponse)
            .build();

        ResponseV3Builder v3Builder = BookieProtocolV3.newResponseBuilder(OperationType.WRITE_LAC);
        v3Builder.getMsgBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(response.getHeader())
            .setWriteLacResponse(WriteLacResponse.newBuilder(writeLacResponse));

        testEncodeDecodeResponse(v3Builder, null, null, response);
    }

    @Test
    public void testReadLac() throws Exception {
        ByteBuf entry = Unpooled.wrappedBuffer("test-read-entry-request".getBytes(UTF_8));
        ByteBuf lacBody = Unpooled.wrappedBuffer("lac-body".getBytes(UTF_8));
        ReadLacResponse readResponse = ReadLacResponse.newBuilder()
            .setStatus(StatusCode.EOK)
            .setLedgerId(3456L)
            .setLacBody(ByteString.copyFrom(lacBody.nioBuffer()))
            .setLastEntryBody(ByteString.copyFrom(entry.nioBuffer()))
            .build();
        Response response = Response.newBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(1234L)
                .setOperation(OperationType.READ_LAC)
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setPriority(3)
                .build())
            .setReadLacResponse(readResponse)
            .build();

        ResponseV3Builder v3Builder = BookieProtocolV3.newResponseBuilder(OperationType.READ_LAC);
        v3Builder.getMsgBuilder()
            .setStatus(StatusCode.EOK)
            .setHeader(response.getHeader())
            .setReadLacResponse(ReadLacResponse.newBuilder(readResponse)
                .clearLacBody()
                .clearLastEntryBody()
                .setLacBody(UnsafeByteOperations.unsafeWrap(lacBody.nioBuffer()))
                .setLastEntryBody(UnsafeByteOperations.unsafeWrap(entry.nioBuffer())));
        v3Builder.retainBuf(entry);
        v3Builder.retainBuf(lacBody);

        testEncodeDecodeResponse(v3Builder, entry, lacBody, response);
    }

    private void testEncodeDecodeResponse(ResponseV3Builder response,
                                          ByteBuf retainedEntryBuf,
                                          ByteBuf retainedLacBuf,
                                          Response expectedResponse) throws Exception {
        EnDecoder coder = new ResponseEnDecoderV3(null);

        Object serialized = coder.encode(response, PooledByteBufAllocator.DEFAULT);
        assertTrue(serialized instanceof ByteBufList);
        ByteBufList serializedBufList = (ByteBufList) serialized;

        // response is recycled but `retainedBuf` is still alive because `serializedBufList` keeps reference
        assertEquals(0, response.refCnt());
        assertEquals(1, serializedBufList.refCnt());
        if (null != retainedEntryBuf) {
            assertEquals(1, retainedEntryBuf.refCnt());
        }
        if (null != retainedLacBuf) {
            assertEquals(1, retainedLacBuf.refCnt());
        }

        // transfer data from `serializedBufList` to a buffer
        int bufferSize = serializedBufList.readableBytes();
        byte[] data = new byte[bufferSize];
        assertEquals(bufferSize, serializedBufList.getBytes(data));

        // serialized buf list is released which also release `retainedBuf`
        serializedBufList.release();
        assertEquals(0, serializedBufList.refCnt());
        if (null != retainedEntryBuf) {
            assertEquals(0, retainedEntryBuf.refCnt());
        }
        if (null != retainedLacBuf) {
            assertEquals(0, retainedLacBuf.refCnt());
        }

        byte[] expectedData = expectedResponse.toByteArray();
        assertEquals(expectedData.length, data.length);
        assertArrayEquals(
            "Expected data : " + Hex.encodeHexString(expectedData) + "\n"
                    + "Actual data   : " + Hex.encodeHexString(data) + "\n",
            expectedData, data);

        // prepare the buffer for decoding
        ByteBuf packet = Unpooled.wrappedBuffer(data);
        Object deserialized = coder.decode(packet);
        assertTrue(deserialized instanceof ResponseV3);

        ResponseV3 respv3 = (ResponseV3) deserialized;
        // `reqv3` is referencing `packet`. so `packet` refcnt is not 0.
        assertEquals(1, respv3.refCnt());
        assertEquals(expectedResponse, respv3.getUnderlyingResponse());

        // if it is add response, make sure we don't copy data to entry buffer
        if (OperationType.READ_ENTRY == respv3.getOpType()
            || OperationType.READ_LAC == respv3.getOpType()) {
            // retained the add entry buffer
            ByteBuf entryBuffer;
            ByteBuf lacBuffer;
            int expectedPacketRefCnt;
            if (OperationType.READ_ENTRY == respv3.getOpType()) {
                entryBuffer = respv3.retainedEntryFromReadResponse();
                lacBuffer = null;
                expectedPacketRefCnt = 2;
            } else {
                entryBuffer = respv3.retainedLastEntryBufFromReadLacResponse();
                lacBuffer = respv3.retainedLacBufFromReadLacResponse();
                expectedPacketRefCnt = 3;
            }
            // packet refcnt will be incremented by 1
            assertEquals(expectedPacketRefCnt, packet.refCnt());
            assertEquals(1, entryBuffer.refCnt());
            if (null != lacBuffer) {
                assertEquals(1, lacBuffer.refCnt());
            }

            // release the request will decrement refcnt by 1
            respv3.release();
            assertEquals(expectedPacketRefCnt - 1, packet.refCnt());
            assertEquals(1, entryBuffer.refCnt());
            if (null != lacBuffer) {
                assertEquals(1, lacBuffer.refCnt());
            }

            // release entry buffer will eventually release underlying packet buffer
            entryBuffer.release();
            assertEquals(expectedPacketRefCnt - 2, packet.refCnt());
            assertEquals(0, entryBuffer.refCnt());
            if (null != lacBuffer) {
                lacBuffer.release();
                assertEquals(0, packet.refCnt());
                assertEquals(0, lacBuffer.refCnt());
            }
        }
    }

}
