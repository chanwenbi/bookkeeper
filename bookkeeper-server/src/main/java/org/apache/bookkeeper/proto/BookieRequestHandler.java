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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.processor.Requests.Request;
import org.apache.bookkeeper.processor.Responses.Response;
import org.apache.bookkeeper.processor.ServerProcessorContext;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

/**
 * Serverside handler for bookkeeper requests
 */
class BookieRequestHandler extends SimpleChannelHandler {

    static Logger LOG = LoggerFactory.getLogger(BookieRequestHandler.class);

    private final Callback<ServerProcessorContext, ServerProcessorContext> REQUEST_HANDLER =
            new Callback<ServerProcessorContext, ServerProcessorContext>() {

        @Override
        public ServerProcessorContext call(ServerProcessorContext ctx) throws Exception {
            processor.processResponse(ctx).addCallbacks(RESPONSE_HANDLER, ctx);
            return ctx;
        }

    };

    private final Callback<ServerProcessorContext, ServerProcessorContext> RESPONSE_HANDLER =
            new Callback<ServerProcessorContext, ServerProcessorContext>() {

        @Override
        public ServerProcessorContext call(ServerProcessorContext ctx) throws Exception {
            ctx.getChannel().write(ctx.getResponse());
            return ctx;
        }

    };

    private final RequestProcessor<ServerProcessorContext, ServerProcessorContext> processor;
    private final ChannelGroup allChannels;

    BookieRequestHandler(ServerConfiguration conf,
            RequestProcessor<ServerProcessorContext, ServerProcessorContext> processor, ChannelGroup allChannels) {
        this.processor = processor;
        this.allChannels = allChannels;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent e)
            throws Exception {
        allChannels.add(ctx.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable throwable = e.getCause();
        LOG.error("Unhandled exception occurred in I/O thread or handler", throwable);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        LOG.debug("Channel connected {}", e);
    }

    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        LOG.debug("Channel disconnected {}", e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof BookieProtocol.BKLedgerRequest)) {
            ctx.sendUpstream(e);
            return;
        }
        // TODO: possibly move protocol related request into a separate handler.
        BookieProtocol.BKLedgerRequest r = (BookieProtocol.BKLedgerRequest) e.getMessage();

        Channel c = ctx.getChannel();
        Request request = r;
        Response response = ResponseBuilder.buildErrorResponse(BookieProtocol.EBADREQ, r);
        ServerProcessorContext reqCtx = new ServerProcessorContext(request, response, c);
        processor.processRequest(reqCtx).addCallbacks(REQUEST_HANDLER, reqCtx);
    }

}