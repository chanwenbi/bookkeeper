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
import java.util.ArrayList;
import java.util.List;
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
        RequestV3 request = RequestV3.get();

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
        cis.enableAliasing(true);
        try {
            request.packet = packet;
            request.underlyingRequest = Request.parseFrom(cis, registry);
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
        ResponseV3 request = ResponseV3.get();

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
        cis.enableAliasing(true);
        try {
            request.packet = packet;
            request.underlyingResponse = Response.parseFrom(cis, registry);
            return request;
        } catch (IOException ioe) {
            ReferenceCountUtil.safeRelease(request);
            throw ioe;
        }
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

    private static final List<Recycler<RequestV3Builder>> REQ_BUILDERS =
        new ArrayList<>(OperationType.values().length);

    static {
        OperationType[] types = OperationType.values();
        for (int i = 0; i < types.length; i++) {
            final OperationType type = types[i];
            Recycler<RequestV3Builder> recycler = new Recycler<RequestV3Builder>() {
                @Override
                protected RequestV3Builder newObject(Handle<RequestV3Builder> handle) {
                    return new RequestV3Builder(handle, type);
                }
            };

            if (i < REQ_BUILDERS.size()) {
                Recycler<RequestV3Builder> oldRecycler = REQ_BUILDERS.get(i);
                if (null == oldRecycler) {
                    REQ_BUILDERS.set(i, recycler);
                }
            } else {
                REQ_BUILDERS.add(recycler);
            }
        }
    }

    private static final List<Recycler<ResponseV3Builder>> RESP_BUILDERS =
        new ArrayList<>(OperationType.values().length);

    static {
        OperationType[] types = OperationType.values();
        for (int i = 0; i < types.length; i++) {
            final OperationType type = types[i];
            Recycler<ResponseV3Builder> recycler = new Recycler<ResponseV3Builder>() {
                @Override
                protected ResponseV3Builder newObject(Handle<ResponseV3Builder> handle) {
                    return new ResponseV3Builder(handle, type);
                }
            };

            if (i < RESP_BUILDERS.size()) {
                Recycler<ResponseV3Builder> oldRecycler = RESP_BUILDERS.get(i);
                if (null == oldRecycler) {
                    RESP_BUILDERS.set(i, recycler);
                }
            } else {
                RESP_BUILDERS.add(recycler);
            }
        }
    }

    public static RequestV3Builder newRequestBuilder(OperationType type) {
        return REQ_BUILDERS.get(type.getNumber() - 1).get();
    }

    public static ResponseV3Builder newResponseBuilder(OperationType type) {
        return RESP_BUILDERS.get(type.getNumber() - 1).get();
    }

    /**
     * Builder provides recyclable protobuf message builders.
     */
    // CHECKSTYLE.OFF: LineLength
    abstract static class PBMessageBuilder<MsgBuilderT extends GeneratedMessageV3.Builder<MsgBuilderT>, BuilderT extends PBMessageBuilder<MsgBuilderT, BuilderT>>
        extends AbstractReferenceCounted {
    // CHECKSTYLE.ON: LineLength

        private final Handle<BuilderT> handle;
        private final MsgBuilderT msgBuilder;
        private ByteBufList retainedBufList;

        protected PBMessageBuilder(Handle<BuilderT> handle, OperationType opType) {
            this.handle = handle;
            this.msgBuilder = createMessageBuilder(opType);
        }

        abstract MsgBuilderT createMessageBuilder(OperationType opType);

        public MsgBuilderT getMsgBuilder() {
            return msgBuilder;
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
            this.msgBuilder.clear();

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

        RequestV3Builder(Handle<RequestV3Builder> handle, OperationType opType) {
            super(handle, opType);
        }

        Request.Builder createMessageBuilder(OperationType opType) {
            Request.Builder reqBuilder = Request.newBuilder();
            // force create the response builder, so we can reuse the builder after `deallocate`
            switch (opType) {
                case ADD_ENTRY:
                    reqBuilder.getAddRequestBuilder();
                    break;
                case READ_ENTRY:
                    reqBuilder.getReadRequestBuilder();
                    break;
                case AUTH:
                    reqBuilder.getAuthRequestBuilder();
                    break;
                case START_TLS:
                    reqBuilder.getStartTLSRequestBuilder();
                    break;
                case GET_BOOKIE_INFO:
                    reqBuilder.getGetBookieInfoRequestBuilder();
                    break;
                case READ_LAC:
                    reqBuilder.getReadLacRequestBuilder();
                    break;
                case WRITE_LAC:
                    reqBuilder.getWriteLacRequestBuilder();
                    break;
                default:
                    break;
            }
            return reqBuilder;
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

        ResponseV3Builder(Handle<ResponseV3Builder> handle, OperationType opType) {
            super(handle, opType);
        }

        Response.Builder createMessageBuilder(OperationType opType) {
            Response.Builder respBuilder = Response.newBuilder();
            // force create the response builder, so we can reuse the builder after `deallocate`
            switch (opType) {
                case ADD_ENTRY:
                    respBuilder.getAddResponseBuilder();
                    break;
                case READ_ENTRY:
                    respBuilder.getReadResponseBuilder();
                    break;
                case AUTH:
                    respBuilder.getAuthResponseBuilder();
                    break;
                case START_TLS:
                    respBuilder.getStartTLSResponse();
                    break;
                case GET_BOOKIE_INFO:
                    respBuilder.getGetBookieInfoResponse();
                    break;
                case READ_LAC:
                    respBuilder.getReadLacResponse();
                    break;
                case WRITE_LAC:
                    respBuilder.getWriteLacResponse();
                    break;
                default:
                    break;
            }
            return respBuilder;
        }

        @Override
        ResponseV3Builder self() {
            return this;
        }
    }

}
