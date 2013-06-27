/**
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.processor.ChainRequestProcessor;
import org.apache.bookkeeper.processor.RequestContext;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.processor.Requests.AddRequest;
import org.apache.bookkeeper.processor.Requests.ReadRequest;
import org.apache.bookkeeper.processor.Requests.Request;
import org.apache.bookkeeper.processor.ResponseContext;
import org.apache.bookkeeper.processor.Responses.AddResponse;
import org.apache.bookkeeper.processor.Responses.LedgerResponse;
import org.apache.bookkeeper.processor.Responses.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    static final long maxMemory = Runtime.getRuntime().maxMemory() / 5;
    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M

    InetSocketAddress addr;
    AtomicLong totalBytesOutstanding;
    ClientSocketChannelFactory channelFactory;
    OrderedSafeExecutor executor;
    private Timer readTimeoutTimer;
    private final RequestProcessor<RequestContext, ResponseContext> processor;

    ConcurrentHashMap<CompletionKey, AddCompletion> addCompletions = new ConcurrentHashMap<CompletionKey, AddCompletion>();
    ConcurrentHashMap<CompletionKey, ReadCompletion> readCompletions = new ConcurrentHashMap<CompletionKey, ReadCompletion>();

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock
     */
    Queue<GenericCallback<Void>> pendingOps = new ArrayDeque<GenericCallback<Void>>();
    volatile Channel channel = null;

    private enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
            };

    private volatile ConnectionState state;
    private final ClientConfiguration conf;
    
    private final Callback<RequestContext, RequestContext> REQUEST_SENDER = new Callback<RequestContext, RequestContext>() {
        public RequestContext call(final RequestContext ctx) throws Exception {
            try {
                ChannelFuture future = ctx.getChannel().write(ctx.getRequest());
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to bookie {}.", ctx.getRequest(),
                                        channel.getRemoteAddress());
                            }
                        } else {
                            errorOutRequest(ctx.getRequest());
                        }
                    }
                });
            } catch (Throwable e) {
                LOG.warn("operation {} failed", ctx.getRequest(), e);
                errorOutRequest(ctx.getRequest());
            }
            return ctx;
        }
    };

    private final Callback<ResponseContext, ResponseContext> RESPONSE_HANDLER = new Callback<ResponseContext, ResponseContext>() {
        @Override
        public ResponseContext call(ResponseContext ctx) throws Exception {
            final LedgerResponse r = (LedgerResponse) ctx.getResponse();
            executor.submitOrdered(r.getLedgerId(), new SafeRunnable() {
                @Override
                public void safeRun() {
                    if (r instanceof AddResponse) {
                        handleAddResponse((AddResponse) r);
                    } else if (r instanceof ReadResponse) {
                        handleReadResponse((ReadResponse) r);
                    } else {
                        LOG.error("Unexpected response : {}", r);
                    }
                }
            });
            return ctx;
        }
    };

    class ErrorOutCallback implements Callback<Exception, Exception> {

        final Request request;

        public ErrorOutCallback(Request request) {
            this.request = request;
        }

        @Override
        public Exception call(Exception e) throws Exception {
            errorOutRequest(request);
            return null;
        }

    };

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding) {
        this(new ClientConfiguration(), executor, channelFactory, addr, totalBytesOutstanding);
    }
            
    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.totalBytesOutstanding = totalBytesOutstanding;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.readTimeoutTimer = null;
        ChainRequestProcessor<RequestContext, ResponseContext> processor = new ChainRequestProcessor<RequestContext, ResponseContext>();
        processor.add(new ClientProtocolProcessor());
        try {
            processor.initialize(conf);
        } catch (IOException ie) {
            throw new RuntimeException("Failed to initialize processors to process request/response : ", ie);
        }
        this.processor = processor;
    }

    private void connect() {
        LOG.info("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the bookie.
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(this);
        bootstrap.setOption("tcpNoDelay", conf.getClientTcpNoDelay());
        bootstrap.setOption("keepAlive", true);

        ChannelFuture future = bootstrap.connect(addr);

        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                int rc;
                Queue<GenericCallback<Void>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {

                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        LOG.info("Successfully connected to bookie: " + addr);
                        rc = BKException.Code.OK;
                        channel = future.getChannel();
                        state = ConnectionState.CONNECTED;
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.error("Closed before connection completed, clean up: " + addr);
                        future.getChannel().close();
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else {
                        LOG.error("Could not connect to bookie: " + addr);
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                        if (state != ConnectionState.CLOSED) {
                            state = ConnectionState.DISCONNECTED;
                        }
                    }

                    // trick to not do operations under the lock, take the list
                    // of pending ops and assign it to a new variable, while
                    // emptying the pending ops by just assigning it to a new
                    // list
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<GenericCallback<Void>>();
                }

                for (GenericCallback<Void> pendingOp : oldPendingOps) {
                    pendingOp.operationComplete(rc, null);
                }
            }
        });
    }

    void connectIfNeededAndDoOp(GenericCallback<Void> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            completeOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING) {
                        // just return as connection request has already send
                        // and waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            op.operationComplete(opRc, null);
        }

    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     * @param masterKey
     * @param entryId
     * @param lastConfirmed
     * @param macCode
     * @param data
     * @param cb
     * @param ctx
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ChannelBuffer toSend, WriteCallback cb,
                  Object ctx, final int options) {
        // TODO: move to a protocol abstraction interface
        BookieProtocol.BKAddRequest r = new BookieProtocol.BKAddRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                ledgerId, entryId, (short)options, masterKey, toSend);
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(ledgerId, entryId);
        addCompletions.put(completionKey, new AddCompletion(cb, entrySize, ctx));

        sendRequest(r);
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        final CompletionKey key = new CompletionKey(ledgerId, entryId);
        readCompletions.put(key, new ReadCompletion(cb, ctx));

        // TODO: move to a protocol abstraction interface
        final BookieProtocol.BKReadRequest r = new BookieProtocol.BKReadRequest(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                BookieProtocol.FLAG_DO_FENCING, masterKey);

        sendRequest(r);
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        final CompletionKey key = new CompletionKey(ledgerId, entryId);
        readCompletions.put(key, new ReadCompletion(cb, ctx));

        // TODO: move to a protocol abstraction interface
        final BookieProtocol.BKReadRequest r = new BookieProtocol.BKReadRequest(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                BookieProtocol.FLAG_NONE);

        sendRequest(r);
    }

    private void sendRequest(Request r) {
        final RequestContext reqCtx = new RequestContext(r, channel);
        processor.processRequest(reqCtx).addCallbacks(REQUEST_SENDER, new ErrorOutCallback(r));
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        closeInternal(false);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        closeInternal(true);
        try {
            this.processor.uninitialize();
        } catch (IOException e) {
            LOG.warn("Failed to uninitialize processors : ", e);
        }
    }

    private void closeInternal(boolean permanent) {
        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (readTimeoutTimer != null) {
            readTimeoutTimer.stop();
            readTimeoutTimer = null;
        }
    }

    // TODO: we should consolidate the completion key
    void errorOutRequest(final Request request) {
        if (request instanceof AddRequest) {
            AddRequest add = (AddRequest) request;
            errorOutAddKey(new CompletionKey(add.getLedgerId(), add.getEntryId()));
        } else if (request instanceof ReadRequest) {
            ReadRequest read = (ReadRequest) request;
            errorOutReadKey(new CompletionKey(read.getLedgerId(), read.getEntryId()));
        }
    }

    void errorOutReadKey(final CompletionKey key) {
        executor.submitOrdered(key.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                ReadCompletion readCompletion = readCompletions.remove(key);

                if (readCompletion != null) {
                    LOG.error("Could not write  request for reading entry: " + key.entryId + " ledger-id: "
                              + key.ledgerId + " bookie: " + channel.getRemoteAddress());

                    readCompletion.cb.readEntryComplete(BKException.Code.BookieHandleNotAvailableException,
                                                        key.ledgerId, key.entryId, null, readCompletion.ctx);
                }
            }

        });
    }

    void errorOutAddKey(final CompletionKey key) {
        executor.submitOrdered(key.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                AddCompletion addCompletion = addCompletions.remove(key);

                if (addCompletion != null) {
                    String bAddress = "null";
                    if(channel != null)
                        bAddress = channel.getRemoteAddress().toString();
                    LOG.error("Could not write request for adding entry: " + key.entryId + " ledger-id: "
                              + key.ledgerId + " bookie: " + bAddress);

                    addCompletion.cb.writeComplete(BKException.Code.BookieHandleNotAvailableException, key.ledgerId,
                                                   key.entryId, addr, addCompletion.ctx);
                    LOG.error("Invoked callback method: " + key.entryId);
                }
            }

        });

    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries() {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. The one who
        // successfully removes the key from the map is the one responsible for
        // calling the application callback.

        for (CompletionKey key : addCompletions.keySet()) {
            errorOutAddKey(key);
        }

        for (CompletionKey key : readCompletions.keySet()) {
            errorOutReadKey(key);
        }
    }

    /**
     * In the netty pipeline, we need to split packets based on length, so we
     * use the {@link LengthFieldBasedFrameDecoder}. Other than that all actions
     * are carried out in this class, e.g., making sense of received messages,
     * prepending the length to outgoing packets etc.
     */
    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        if (readTimeoutTimer == null) {
            readTimeoutTimer = new HashedWheelTimer();
        }

        pipeline.addLast("readTimeout", new ReadTimeoutHandler(readTimeoutTimer, 
                                                               conf.getReadTimeout()));
        pipeline.addLast("lengthbasedframedecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder());
        pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder());


        pipeline.addLast("mainhandler", this);
        return pipeline;
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        LOG.info("Disconnected from bookie: " + addr);
        errorOutOutstandingEntries();
        Channel c = this.channel;
        if (c != null) {
            c.close();
        }
        synchronized (this) {
            if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable t = e.getCause();
        if (t instanceof CorruptedFrameException || t instanceof TooLongFrameException) {
            LOG.error("Corrupted fram received from bookie: "
                      + e.getChannel().getRemoteAddress());
            return;
        }
        if (t instanceof ReadTimeoutException) {
            for (CompletionKey key : addCompletions.keySet()) {
                if (key.shouldTimeout()) {
                    errorOutAddKey(key);
                }
            }
            for (CompletionKey key : readCompletions.keySet()) {
                if (key.shouldTimeout()) {
                    errorOutReadKey(key);
                }
            }
            return;
        }

        if (t instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            return;
        }

        LOG.error("Unexpected exception caught by bookie client channel handler", t);
        // Since we are a library, cant terminate App here, can we?
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof LedgerResponse)) {
            ctx.sendUpstream(e);
            return;
        }
        final LedgerResponse r = (LedgerResponse) e.getMessage();
        processor.processResponse(new ResponseContext(r, e.getChannel())).addCallback(RESPONSE_HANDLER);
    }

    void handleAddResponse(AddResponse a) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: {} for ledger: {}", addr, a);
        }

        AddCompletion ac;
        ac = addCompletions.remove(new CompletionKey(a.getLedgerId(),
                                                     a.getEntryId()));
        if (ac == null) {
            LOG.error("Unexpected add response from bookie {} for {}", addr, a);
            return;
        }

        ac.cb.writeComplete(a.getErrorCode(), a.getLedgerId(), a.getEntryId(), addr, ac.ctx);
    }

    void handleReadResponse(ReadResponse rr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request {} entry length: {}", rr,
                    rr.getDataAsChannelBuffer() != null ? rr.getDataAsChannelBuffer().readableBytes() : -1);
        }

        CompletionKey key = new CompletionKey(rr.getLedgerId(), rr.getEntryId());
        ReadCompletion readCompletion = readCompletions.remove(key);

        if (readCompletion == null) {
            /*
             * This is a special case. When recovering a ledger, a client
             * submits a read request with id -1, and receives a response with a
             * different entry id.
             */
            
            readCompletion = readCompletions.remove(new CompletionKey(rr.getLedgerId(),
                                                                      BookieProtocol.LAST_ADD_CONFIRMED));
        }

        if (readCompletion == null) {
            LOG.error("Unexpected read response received from bookie: {} for {}", addr, rr);
            return;
        }

        readCompletion.cb.readEntryComplete(rr.getErrorCode(), rr.getLedgerId(), rr.getEntryId(),
                                            rr.getDataAsChannelBuffer(), readCompletion.ctx);
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */
    // visible for testing
    static class ReadCompletion {
        final ReadEntryCallback cb;
        final Object ctx;

        public ReadCompletion(ReadEntryCallback cb, Object ctx) {
            this.cb = cb;
            this.ctx = ctx;
        }
    }

    // visible for testing
    static class AddCompletion {
        final WriteCallback cb;
        //final long size;
        final Object ctx;

        public AddCompletion(WriteCallback cb, long size, Object ctx) {
            this.cb = cb;
            //this.size = size;
            this.ctx = ctx;
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long ledgerId, long entryId) {
        return new CompletionKey(ledgerId, entryId);
    }

    // visable for testing
    class CompletionKey {
        long ledgerId;
        long entryId;
        final long timeoutAt;

        CompletionKey(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.timeoutAt = MathUtils.now() + (conf.getReadTimeout()*1000);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.ledgerId == that.ledgerId && this.entryId == that.entryId;
        }

        @Override
        public int hashCode() {
            return ((int) ledgerId << 16) ^ ((int) entryId);
        }

        public String toString() {
            return String.format("LedgerEntry(%d, %d)", ledgerId, entryId);
        }

        public boolean shouldTimeout() {
            return this.timeoutAt <= MathUtils.now();
        }
    }

}
