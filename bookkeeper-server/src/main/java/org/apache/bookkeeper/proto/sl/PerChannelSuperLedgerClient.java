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
package org.apache.bookkeeper.proto.sl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadAcctMetaResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadDataEntryResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.WriteAcctMetaResponse;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
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
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */

@ChannelPipelineCoverage("one")
public class PerChannelSuperLedgerClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelSuperLedgerClient.class);

    static final long maxMemory = Runtime.getRuntime().maxMemory() / 5;
    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M

    InetSocketAddress addr;
    ClientSocketChannelFactory channelFactory;
    OrderedSafeExecutor executor;
    private Timer readTimeoutTimer;
    private AtomicLong txnGenerator = new AtomicLong(0);

    final ConcurrentHashMap<CompletionKey, Completion> completions = new ConcurrentHashMap<CompletionKey, Completion>();

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

    public PerChannelSuperLedgerClient(OrderedSafeExecutor executor,
            ClientSocketChannelFactory channelFactory, InetSocketAddress addr) {
        this(new ClientConfiguration(), executor, channelFactory, addr);
    }
            
    public PerChannelSuperLedgerClient(ClientConfiguration conf, OrderedSafeExecutor executor,
            ClientSocketChannelFactory channelFactory, InetSocketAddress addr) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.readTimeoutTimer = null;
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

                synchronized (PerChannelSuperLedgerClient.this) {

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

    void sendRequest(ByteString ledgerName, int epoch, int orderKey, SimpleRequest.Builder requestBuilder,
            RequestCallback cb, Object ctx) {
        long txnId = txnGenerator.incrementAndGet();
        requestBuilder.getHeaderBuilder().setTxnId(txnId);
        final CompletionKey completionKey = new CompletionKey(ledgerName, txnId, orderKey);
        final Completion completion = new Completion(cb, ctx);
        completions.put(completionKey, completion);
        // TODO: maybe we could batch the requests in future.
        final SuperLedgerRequest slreq = SuperLedgerRequest.newBuilder().setLedgerName(ledgerName)
                .setEpoch(epoch).addRequests(requestBuilder).build();
        try {
            ChannelFuture future = channel.write(slreq);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request "
                                    + SuperLedgerUtils.requestToReadbleString(slreq) + " to bookie: "
                                    + channel.getRemoteAddress());
                        }
                    } else {
                        errorOutKey(completionKey);
                    }
                }
            });
        } catch (Throwable t) {
            LOG.warn("Send request " + SuperLedgerUtils.requestToReadbleString(slreq) + " failed : ", t);
            errorOutKey(completionKey);
        }
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

    void errorOutKey(final CompletionKey key) {
        executor.submitOrdered(key.orderKey, new SafeRunnable() {

            @Override
            public void safeRun() {
                Completion completion = completions.remove(key);
                if (null != completion) {
                    LOG.error("Could not write request for request {} to bookie {}.", key.txnId,
                            channel.getRemoteAddress());
                    completion.cb.requestComplete(StatusCode.EBOOKIENOTAVAILABLE, key.ledgerName, null, addr,
                            completion.ctx);
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

        for (CompletionKey key : completions.keySet()) {
            errorOutKey(key);
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
        pipeline.addLast("ledgthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("protobufdecoder", new ProtobufDecoder(SuperLedgerResponse.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());
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
            for (CompletionKey key : completions.keySet()) {
                if (key.shouldTimeout()) {
                    errorOutKey(key);
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
        if (!(e.getMessage() instanceof SuperLedgerResponse)) {
            ctx.sendUpstream(e);
            return;
        }

        SuperLedgerResponse slr = (SuperLedgerResponse) e.getMessage();
        ByteString ledgerName = slr.getLedgerName();
        for (SimpleResponse sr : slr.getResponsesList()) {
            handleResponse(ledgerName, sr);
        }
    }

    private void handleResponse(final ByteString ledgerName, SimpleResponse sr) {

        final long txnId = sr.getHeader().getTxnId();
        OperationType opCode = sr.getHeader().getType();
        final StatusCode status = sr.getStatus();
        ByteString acctName = null;
        Object response = null;
        switch (opCode) {
        case ADD_DATA_ENTRY:
            AddDataEntryResponse addResp = sr.getAddEntryResponse();
            acctName = addResp.getAcctName();
            response = addResp;
            break;
        case READ_DATA_ENTRY:
            ReadDataEntryResponse readResp = sr.getReadEntryResponse();
            acctName = readResp.getAcctName();
            response = readResp;
            break;
        case WRITE_ACCT_META:
            WriteAcctMetaResponse writeResp = sr.getWriteAcctResponse();
            acctName = writeResp.getAcctName();
            response = writeResp;
            break;
        case READ_ACCT_META:
            ReadAcctMetaResponse readAcctResp = sr.getReadAcctResponse();
            acctName = readAcctResp.getAcctName();
            response = readAcctResp;
            break;
        default:
            LOG.error("Unexepected response, type: " + opCode + ", txnId: " + txnId
                    + " received from bookie: " + addr + ", ignoring");
            return;
        }
        final int orderKey = SuperLedgerUtils.hashCode(ledgerName, acctName);
        final Object retValue = response;

        executor.submitOrdered(orderKey, new SafeRunnable() {

            @Override
            public void safeRun() {
                Completion ac = completions.remove(new CompletionKey(ledgerName, txnId, orderKey));
                if (null == ac) {
                    LOG.error("Unexpected add response received from bookie: " + addr
                            + " for request(ledger:" + ledgerName.toStringUtf8() + ", txn:" + txnId
                            + "), ignoring... ");
                    return;
                }
                ac.cb.requestComplete(status, ledgerName, retValue, addr, ac.ctx);
            }

        });

    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */
    static class Completion {
        final RequestCallback cb;
        final Object ctx;

        public Completion(RequestCallback cb, Object ctx) {
            this.cb = cb;
            this.ctx = ctx;
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(ByteString ledgerName, long txnId, int orderKey) {
        return new CompletionKey(ledgerName, txnId, orderKey);
    }

    // visable for testing
    class CompletionKey {
        final ByteString ledgerName;
        final long txnId;
        final long timeoutAt;
        final int orderKey;

        CompletionKey(ByteString ledgerName, long txnId, int orderKey) {
            this.ledgerName = ledgerName;
            this.txnId = txnId;
            this.orderKey = orderKey;
            this.timeoutAt = MathUtils.now() + (conf.getReadTimeout()*1000);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.txnId == that.txnId;
        }

        @Override
        public int hashCode() {
            return (int) txnId;
        }

        public String toString() {
            return String.format("Request(%s, txn: %d)", ledgerName.toStringUtf8(), txnId);
        }

        public boolean shouldTimeout() {
            return this.timeoutAt <= MathUtils.now();
        }
    }

}
