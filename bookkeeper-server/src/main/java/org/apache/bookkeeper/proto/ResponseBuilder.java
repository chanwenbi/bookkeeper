/**
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.protobuf.ByteString;

class ResponseBuilder {
    private static BookieProtocol.Response buildErrorResponse(int errorCode, BookieProtocol.Request r) {
        if (r.getOpCode() == BookieProtocol.ADDENTRY) {
            return new BookieProtocol.AddResponse(r.getProtocolVersion(), errorCode,
                                                  r.getLedgerId(), r.getEntryId());
        } else {
            assert(r.getOpCode() == BookieProtocol.READENTRY);
            return new BookieProtocol.ReadResponse(r.getProtocolVersion(), errorCode,
                                                   r.getLedgerId(), r.getEntryId());
        }
    }

    private static BookkeeperProtocol.Response buildErrorResponseV3(BookkeeperProtocol.StatusCode errorCode,
                    BookkeeperProtocol.Request r) {
        BookkeeperProtocol.Response.Builder respBuilder =
                        BookkeeperProtocol.Response.newBuilder().setHeader(r.getHeader()).setStatus(errorCode);
        if (r.hasAddRequest()) {
            BookkeeperProtocol.AddRequest add = r.getAddRequest();
            BookkeeperProtocol.AddResponse.Builder addRespBuilder =
                            BookkeeperProtocol.AddResponse.newBuilder().setStatus(errorCode)
                                            .setLedgerId(add.getEntryId()).setEntryId(add.getEntryId());
            respBuilder.setAddResponse(addRespBuilder);
        } else if (r.hasReadRequest()) {
            BookkeeperProtocol.ReadRequest add = r.getReadRequest();
            BookkeeperProtocol.ReadResponse.Builder readRespBuilder =
                            BookkeeperProtocol.ReadResponse.newBuilder().setStatus(errorCode)
                                            .setLedgerId(add.getEntryId()).setEntryId(add.getEntryId());
            respBuilder.setReadResponse(readRespBuilder);
        }
        return respBuilder.build();
    }

    static Object buildErrorResponse(BookkeeperProtocol.StatusCode errorCode,
                    BookkeeperProtocol.Request r) {
        if (r.getHeader().getVersion().equals(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)) {
            return buildErrorResponseV3(errorCode, r);
        } else {
            // TODO:
            return null;
        }
    }

    static BookieProtocol.Response buildAddResponse(BookieProtocol.Request r) {
        return new BookieProtocol.AddResponse(r.getProtocolVersion(), BookieProtocol.EOK, r.getLedgerId(),
                                              r.getEntryId());
    }

    private static BookkeeperProtocol.Response buildAddResponseV3(BookkeeperProtocol.Request r) {
        BookkeeperProtocol.AddResponse.Builder addRespBuilder =
                        BookkeeperProtocol.AddResponse.newBuilder().setStatus(StatusCode.EOK)
                                        .setLedgerId(r.getAddRequest().getLedgerId())
                                        .setEntryId(r.getAddRequest().getEntryId());
        return BookkeeperProtocol.Response.newBuilder().setHeader(r.getHeader()).setStatus(StatusCode.EOK)
                        .setAddResponse(addRespBuilder).build();
    }

    static Object buildAddResponse(BookkeeperProtocol.Request r) {
        if (r.getHeader().getVersion().equals(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)) {
            return buildAddResponseV3(r);
        } else {
            // TODO:
            return null;
        }
    }

    static BookieProtocol.Response buildReadResponse(ByteBuffer data, BookieProtocol.Request r) {
        return new BookieProtocol.ReadResponse(r.getProtocolVersion(), BookieProtocol.EOK,
                r.getLedgerId(), r.getEntryId(), ChannelBuffers.wrappedBuffer(data));
    }

    private static BookkeeperProtocol.Response buildReadResponseV3(ByteBuffer data, BookkeeperProtocol.Request r) {
        BookkeeperProtocol.ReadResponse.Builder readRespBuilder =
                        BookkeeperProtocol.ReadResponse.newBuilder().setStatus(StatusCode.EOK)
                                        .setLedgerId(r.getReadRequest().getLedgerId())
                                        .setEntryId(r.getReadRequest().getEntryId())
                                        .setBody(ByteString.copyFrom(data));
        return BookkeeperProtocol.Response.newBuilder().setHeader(r.getHeader()).setStatus(StatusCode.EOK)
                        .setReadResponse(readRespBuilder).build();
    }

    static Object buildReadResponse(ByteBuffer data, BookkeeperProtocol.Request r) {
        if (r.getHeader().getVersion().equals(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)) {
            return buildReadResponseV3(data, r);
        } else {
            // TODO:
            return null;
        }
    }
}
