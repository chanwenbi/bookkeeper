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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedStreamUtil;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookieProtoEncoding.EnDecoder;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ByteBufSideCar;
import org.apache.bookkeeper.util.RecycableByteOutput;

/**
 * The protobuf based v3 protocol.
 */
@Slf4j
public final class BookieProtocolV3 {

    private BookieProtocolV3() {}

    /**
     * A request encoder/decoder for v3 protobuf request.
     */
    public static class RequestEnDecoderV3 implements EnDecoder {

        final ExtensionRegistry extensionRegistry;

        public RequestEnDecoderV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object object, ByteBufAllocator allocator) throws Exception {
            RequestV3Builder reqBuilder = (RequestV3Builder) object;
            try {
                return reqBuilder.serialize(allocator);
            } finally {
                reqBuilder.release();
            }
        }

        @Override
        public Object decode(ByteBuf packet) throws Exception {
            return parseRequest(packet, extensionRegistry);
        }
    }

    /**
     * A response encoder/decoder for v3 protobuf response.
     */
    public static class ResponseEnDecoderV3 implements EnDecoder {

        final ExtensionRegistry extensionRegistry;

        public ResponseEnDecoderV3(ExtensionRegistry extensionRegistry) {
            this.extensionRegistry = extensionRegistry;
        }

        @Override
        public Object encode(Object object, ByteBufAllocator allocator) throws Exception {
            ResponseV3Builder respBuilder = (ResponseV3Builder) object;
            try {
                return respBuilder.serialize(allocator);
            } finally {
                respBuilder.release();
            }
        }

        @Override
        public Object decode(ByteBuf packet) throws Exception {
            return parseResponse(packet, extensionRegistry);
        }
    }


    /**
     * Parse a v3 request from a provided {@link ByteBuf} <tt>packet</tt>.
     *
     * @param packet packet to parse
     * @param registry extension registry that protobuf uses on parsing.
     * @return a v3 request object.
     * @throws IOException
     */
    public static RequestV3 parseRequest(ByteBuf packet, ExtensionRegistry registry) throws IOException {
        return parseRequest(packet, registry, true);
    }

    static RequestV3 parseRequest(ByteBuf packet, ExtensionRegistry registry, boolean enableAliasing)
            throws IOException {
        RequestV3 request = RequestV3.get();

        try {
            request.packet = packet;
            request.underlyingRequest = parseMessage(
                Request.parser(),
                packet,
                registry,
                enableAliasing);
            return request;
        } catch (IOException ioe) {
            ReferenceCountUtil.safeRelease(request);
            throw ioe;
        }
    }

    /**
     * Parse a v3 response from a provided {@link ByteBuf} <tt>packet</tt>.
     *
     * @param packet packet to parse
     * @param registry extension registry that protobuf uses on parsing.
     * @return a v3 response object.
     * @throws IOException
     */
    public static ResponseV3 parseResponse(ByteBuf packet, ExtensionRegistry registry) throws IOException {
        return parseResponse(packet, registry, true);
    }

    static ResponseV3 parseResponse(ByteBuf packet, ExtensionRegistry registry, boolean enableAliasing)
            throws IOException {
        ResponseV3 response = ResponseV3.get();

        try {
            response.packet = packet;
            response.underlyingResponse = parseMessage(
                Response.parser(),
                packet,
                registry,
                enableAliasing);
            return response;
        } catch (IOException ioe) {
            ReferenceCountUtil.safeRelease(response);
            throw ioe;
        }
    }

    static <T> T parseMessage(Parser<T> parser,
                              ByteBuf packet,
                              ExtensionRegistry registry,
                              boolean enableAliasing)
            throws IOException {
        CodedInputStream cis;
        if (packet.hasArray()) {
            cis = CodedStreamUtil.fromImmutableArray(
                packet.array(),
                packet.arrayOffset() + packet.readerIndex(),
                packet.readableBytes());
        } else if (packet.isDirect()) {
            cis = CodedStreamUtil.fromNioBuffer(packet.nioBuffer());
        } else {
            ByteBufInputStream is = new ByteBufInputStream(packet);
            cis = CodedInputStream.newInstance(is);
        }
        // enable aliasing - so ByteString will not copy data from `packet`.
        cis.enableAliasing(enableAliasing);
        return parser.parseFrom(cis, registry);
    }

    /**
     * A wrap class wrapping over the underlying request.
     *
     * <p>It can be used for holding the underlying bytebuf for protobuf messages in future.
     */
    public static class RequestV3 extends AbstractReferenceCounted {

        private static final Recycler<RequestV3> RECYCLER = new Recycler<RequestV3>() {
            @Override
            protected RequestV3 newObject(Handle<RequestV3> handle) {
                return new RequestV3(handle);
            }
        };

        private static RequestV3 get() {
            RequestV3 req = RECYCLER.get();
            req.setRefCnt(1);
            return req;
        }

        private final Handle<RequestV3> handle;
        // the deserialized request
        private Request underlyingRequest;
        // the packet contains serialized data
        private ByteBuf packet;

        private RequestV3(Handle<RequestV3> handle) {
            this.handle = handle;
        }

        public Request getUnderlyingRequest() {
            return underlyingRequest;
        }

        public boolean isVersionCompatible() {
            return null != underlyingRequest
                && underlyingRequest.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
        }

        public OperationType getOpType() {
            return underlyingRequest.getHeader().getOperation();
        }

        public long getTxnId() {
            return underlyingRequest.getHeader().getTxnId();
        }

        public ByteBuf retainedEntryFromAddRequest() {
            if (underlyingRequest.hasAddRequest()) {
                ByteBuf entryBuf =
                    Unpooled.wrappedBuffer(underlyingRequest.getAddRequest().getBody().asReadOnlyByteBuffer());
                // the content of entryBuf is backed by the `packet`, so construct a `ByteBufSideCar`
                ByteBufSideCar result = ByteBufSideCar.get(entryBuf);
                result.addToSideCar(packet.retain());
                return result;
            } else {
                return null;
            }
        }

        public AddRequest getAddRequest() {
            return underlyingRequest.getAddRequest();
        }

        public ReadRequest getReadRequest() {
            return underlyingRequest.getReadRequest();
        }

        public ReadLacRequest getReadLacRequest() {
            return underlyingRequest.getReadLacRequest();
        }

        public ByteBuf retainedLacBufFromWriteLacRequest() {
            if (underlyingRequest.hasWriteLacRequest()) {
                ByteBuf entryBuf =
                    Unpooled.wrappedBuffer(underlyingRequest.getWriteLacRequest().getBody().asReadOnlyByteBuffer());
                // the content of entryBuf is backed by the `packet`, so construct a `ByteBufSideCar`
                ByteBufSideCar result = ByteBufSideCar.get(entryBuf);
                result.addToSideCar(packet.retain());
                return result;
            } else {
                return null;
            }
        }

        public WriteLacRequest getWriteLacRequest() {
            return underlyingRequest.getWriteLacRequest();
        }

        public GetBookieInfoRequest getGetBookieInfoRequest() {
            return underlyingRequest.getGetBookieInfoRequest();
        }

        @Override
        protected void deallocate() {
            if (null != packet) {
                ReferenceCountUtil.safeRelease(packet);
                packet = null;
            }
            underlyingRequest = null;

            handle.recycle(this);
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

    }

    /**
     * A wrap class wrapping over the underlying request.
     *
     * <p>It can be used for holding the underlying bytebuf for protobuf messages in future.
     */
    public static class ResponseV3 extends AbstractReferenceCounted {

        private static final Recycler<ResponseV3> RECYCLER = new Recycler<ResponseV3>() {
            @Override
            protected ResponseV3 newObject(Handle<ResponseV3> handle) {
                return new ResponseV3(handle);
            }
        };

        private static ResponseV3 get() {
            ResponseV3 req = RECYCLER.get();
            req.setRefCnt(1);
            return req;
        }

        private final Handle<ResponseV3> handle;
        // the deserialized response
        private Response underlyingResponse;
        // the packet contains serialized data
        private ByteBuf packet;

        private ResponseV3(Handle<ResponseV3> handle) {
            this.handle = handle;
        }

        public Response getUnderlyingResponse() {
            return underlyingResponse;
        }

        public boolean isVersionCompatible() {
            return null != underlyingResponse
                && underlyingResponse.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
        }

        public OperationType getOpType() {
            return underlyingResponse.getHeader().getOperation();
        }

        public long getTxnId() {
            return underlyingResponse.getHeader().getTxnId();
        }

        public AddResponse getAddResponse() {
            return underlyingResponse.getAddResponse();
        }

        public ByteBuf retainedEntryFromReadResponse() {
            if (underlyingResponse.hasReadResponse()) {
                ByteBuf entryBuf =
                    Unpooled.wrappedBuffer(underlyingResponse.getReadResponse().getBody().asReadOnlyByteBuffer());
                // the content of entryBuf is backed by the `packet`, so construct a `ByteBufSideCar`
                ByteBufSideCar result = ByteBufSideCar.get(entryBuf);
                result.addToSideCar(packet.retain());
                return result;
            } else {
                return null;
            }
        }

        public ReadResponse getReadResponse() {
            return underlyingResponse.getReadResponse();
        }

        public ByteBuf retainedLacBufFromReadLacResponse() {
            if (underlyingResponse.hasReadLacResponse()) {
                ByteBuf entryBuf =
                    Unpooled.wrappedBuffer(underlyingResponse.getReadLacResponse().getLacBody().asReadOnlyByteBuffer());
                // the content of entryBuf is backed by the `packet`, so construct a `ByteBufSideCar`
                ByteBufSideCar result = ByteBufSideCar.get(entryBuf);
                result.addToSideCar(packet.retain());
                return result;
            } else {
                return null;
            }
        }

        public ByteBuf retainedLastEntryBufFromReadLacResponse() {
            if (underlyingResponse.hasReadLacResponse()) {
                ByteBuf entryBuf = Unpooled.wrappedBuffer(
                    underlyingResponse.getReadLacResponse().getLastEntryBody().asReadOnlyByteBuffer());
                // the content of entryBuf is backed by the `packet`, so construct a `ByteBufSideCar`
                ByteBufSideCar result = ByteBufSideCar.get(entryBuf);
                result.addToSideCar(packet.retain());
                return result;
            } else {
                return null;
            }
        }

        public ReadLacResponse getReadLacResponse() {
            return underlyingResponse.getReadLacResponse();
        }

        public WriteLacResponse getWriteLacResponse() {
            return underlyingResponse.getWriteLacResponse();
        }

        public GetBookieInfoResponse getGetBookieInfoResponse() {
            return underlyingResponse.getGetBookieInfoResponse();
        }

        @Override
        protected void deallocate() {
            if (null != packet) {
                ReferenceCountUtil.safeRelease(packet);
                packet = null;
            }
            underlyingResponse = null;

            handle.recycle(this);
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

    }

    /**
     * Builder provides recyclable protobuf message builders.
     */
    // CHECKSTYLE.OFF: LineLength
    abstract static class PBMessageBuilder<MsgBuilderT extends GeneratedMessageV3.Builder<MsgBuilderT>, BuilderT extends PBMessageBuilder<MsgBuilderT, BuilderT>>
        extends AbstractReferenceCounted {
    // CHECKSTYLE.ON: LineLength

        private final Handle<BuilderT> handle;
        private MsgBuilderT msgBuilder;
        private ByteBufList retainedBufList;

        protected PBMessageBuilder(Handle<BuilderT> handle) {
            this.handle = handle;
        }

        public MsgBuilderT getMsgBuilder() {
            return msgBuilder;
        }

        public BuilderT setMsgBuilder(MsgBuilderT builder) {
            this.msgBuilder = builder;
            return self();
        }

        public void retainBuf(ByteBuf buf) {
            if (null != retainedBufList) {
                retainedBufList.add(buf);
            } else {
                retainedBufList = ByteBufList.get(buf);
            }
        }

        public int getRetainedBytes() {
            if (null == retainedBufList) {
                return 0;
            } else {
                return retainedBufList.readableBytes();
            }
        }

        abstract BuilderT self();

        @Override
        protected void deallocate() {
            if (null != msgBuilder) {
                this.msgBuilder.clear();
            }

            if (null != retainedBufList) {
                ReferenceCountUtil.safeRelease(retainedBufList);
                retainedBufList = null;
            }

            handle.recycle(self());
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

        public ByteBufList serialize(ByteBufAllocator allocator) {
            return serializeLazy(allocator);
        }

        ByteBufList serializeHeap(ByteBufAllocator allocator) {
            MessageLite msg = msgBuilder.build();

            int len = msg.getSerializedSize();

            ByteBuf buf = allocator.heapBuffer(len, len);
            buf.writerIndex(buf.readerIndex() + len);

            byte[] array = buf.array();
            int offset = buf.arrayOffset() + buf.readerIndex();

            CodedOutputStream os = CodedOutputStream.newInstance(array, offset, len);
            try {
                msg.writeTo(os);
                os.flush();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            return ByteBufList.get(buf);
        }

        ByteBufList serializeDirect(ByteBufAllocator allocator) {
            MessageLite msg = msgBuilder.build();

            int len = msg.getSerializedSize();

            ByteBuf buf = allocator.directBuffer(len, len);
            buf.writerIndex(msg.getSerializedSize());
            CodedOutputStream os = CodedOutputStream.newInstance(buf.nioBuffer());
            try {
                msg.writeTo(os);
                os.flush();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            return ByteBufList.get(buf);
        }

        ByteBufList serializeLazy(ByteBufAllocator allocator) {
            MessageLite msg = msgBuilder.build();

            // estimate the buffer
            int msgSize = msg.getSerializedSize();
            int bytesSize = getRetainedBytes();
            int nonBytesSize = Math.max(32, msgSize - bytesSize);

            if (log.isTraceEnabled()) {
                log.trace("msg size : {}, bytes size : {}, non bytes size : {}",
                    msgSize, bytesSize, nonBytesSize);
            }

            RecycableByteOutput output = RecycableByteOutput.create(nonBytesSize, allocator);
            try {
                CodedOutputStream os = CodedStreamUtil.fromByteOutput(output, nonBytesSize);
                msg.writeTo(os);
                os.flush();

                ByteBufList bufList = output.complete();
                bufList.setSidecarList(ReferenceCountUtil.retain(retainedBufList));
                return bufList;
            } catch (IOException e) {
                // This is in-memory serialization, should not fail
                throw new RuntimeException(e);
            } finally {
                output.recycle();
            }
        }


    }

    /**
     * Builder provides recyclable protobuf message builders.
     */
    public static class RequestV3Builder extends PBMessageBuilder<Request.Builder, RequestV3Builder> {

        private static final Recycler<RequestV3Builder> RECYCLER = new Recycler<RequestV3Builder>() {
            @Override
            protected RequestV3Builder newObject(Handle<RequestV3Builder> handle) {
                return new RequestV3Builder(handle);
            }
        };

        public static RequestV3Builder get() {
            RequestV3Builder builder = RECYCLER.get();
            builder.setRefCnt(1);
            return builder;
        }

        RequestV3Builder(Handle<RequestV3Builder> handle) {
            super(handle);
        }

        @Override
        RequestV3Builder self() {
            return this;
        }

    }

    /**
     * Builder provides recyclable protobuf message builders.
     */
    public static class ResponseV3Builder extends PBMessageBuilder<Response.Builder, ResponseV3Builder> {

        private static final Recycler<ResponseV3Builder> RECYCLER = new Recycler<ResponseV3Builder>() {
            @Override
            protected ResponseV3Builder newObject(Handle<ResponseV3Builder> handle) {
                return new ResponseV3Builder(handle);
            }
        };

        public static ResponseV3Builder get() {
            ResponseV3Builder builder = RECYCLER.get();
            builder.setRefCnt(1);
            return builder;
        }

        ResponseV3Builder(Handle<ResponseV3Builder> handle) {
            super(handle);
        }

        @Override
        ResponseV3Builder self() {
            return this;
        }
    }

}
