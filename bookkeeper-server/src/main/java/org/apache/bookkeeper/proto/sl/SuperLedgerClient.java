package org.apache.bookkeeper.proto.sl;

/*
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryRequest.PayLoad;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Implements the client-side part of the SuperLedger protocol.
 * 
 */
public class SuperLedgerClient {
    static final Logger LOG = LoggerFactory.getLogger(SuperLedgerClient.class);

    // This is global state that should be across all BookieClients
    AtomicLong totalBytesOutstanding = new AtomicLong();

    OrderedSafeExecutor executor;
    ClientSocketChannelFactory channelFactory;
    ConcurrentHashMap<InetSocketAddress, PerChannelSuperLedgerClient> channels = new ConcurrentHashMap<InetSocketAddress, PerChannelSuperLedgerClient>();

    private final ClientConfiguration conf;
    private volatile boolean closed;
    private ReentrantReadWriteLock closeLock;

    public SuperLedgerClient(ClientConfiguration conf, ClientSocketChannelFactory channelFactory,
            OrderedSafeExecutor executor) {
        this.conf = conf;
        this.channelFactory = channelFactory;
        this.executor = executor;
        this.closed = false;
        this.closeLock = new ReentrantReadWriteLock();
    }

    public PerChannelSuperLedgerClient lookupClient(InetSocketAddress addr) {
        PerChannelSuperLedgerClient channel = channels.get(addr);

        if (channel == null) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return null;
                }
                channel = new PerChannelSuperLedgerClient(conf, executor, channelFactory, addr);
                PerChannelSuperLedgerClient prevChannel = channels.putIfAbsent(addr, channel);
                if (prevChannel != null) {
                    channel = prevChannel;
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }

        return channel;
    }

    public void closeClients(Set<InetSocketAddress> addrs) {
        final HashSet<PerChannelSuperLedgerClient> clients = new HashSet<PerChannelSuperLedgerClient>();
        for (InetSocketAddress a : addrs) {
            PerChannelSuperLedgerClient c = channels.get(a);
            if (c != null) {
                clients.add(c);
            }
        }

        if (clients.size() == 0) {
            return;
        }
        executor.submit(new SafeRunnable() {
                @Override
                public void safeRun() {
                for (PerChannelSuperLedgerClient c : clients) {
                        c.disconnect();
                    }
                }
            });
    }

    public void sendRequest(final InetSocketAddress addr, final ByteString ledgerName, final int epoch,
            final int orderKey, final SimpleRequest.Builder requestBuilder, final RequestCallback cb,
            final Object ctx) {
        final PerChannelSuperLedgerClient client = lookupClient(addr);
        if (client == null) {
            executor.submitOrdered(orderKey, new SafeRunnable() {

                @Override
                public void safeRun() {
                    cb.requestComplete(StatusCode.EBOOKIENOTAVAILABLE, ledgerName, null, addr, ctx);
                }

            });
            return;
        }

        client.connectIfNeededAndDoOp(new GenericCallback<Void>() {
            @Override
            public void operationComplete(final int rc, Void result) {
                if (rc != BKException.Code.OK) {
                    executor.submitOrdered(orderKey, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            cb.requestComplete(StatusCode.EBOOKIENOTAVAILABLE, ledgerName, null, addr, ctx);
                        }
                    });
                    return;
                }
                client.sendRequest(ledgerName, epoch, orderKey, requestBuilder, cb, ctx);
            }
        });
    }

    public void close() {
        closeLock.writeLock().lock();
        try {
            closed = true;
            for (PerChannelSuperLedgerClient channel : channels.values()) {
                channel.close();
            }
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    private static class Counter {
        int i;
        int total;

        synchronized void inc() {
            i++;
            total++;
        }

        synchronized void dec() {
            i--;
            notifyAll();
        }

        synchronized void wait(int limit) throws InterruptedException {
            while (i > limit) {
                wait();
            }
        }

        synchronized int total() {
            return total;
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws NumberFormatException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
        if (args.length != 4) {
            System.err.println("USAGE: SuperLedgerClient bookieHost port ledgerName acctName#");
            return;
        }
        final ByteString hello = ByteString.copyFromUtf8("hello");
        final ByteString masterKey = ByteString.copyFromUtf8("test");
        final ByteString ledgerName = ByteString.copyFromUtf8(args[2]);
        final ByteString acctName = ByteString.copyFromUtf8(args[3]);

        RequestCallback<AddDataEntryResponse> cb = new RequestCallback<AddDataEntryResponse>() {

            @Override
            public void requestComplete(StatusCode code, ByteString ledgerName,
                    AddDataEntryResponse response, InetSocketAddress addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                if (null != response) {
                    System.out.println("rc= " + code + " for " + response.getEntriesList() + "@(ledger:"
                            + ledgerName + ", acct:" + acctName + ")");
                }
            }

        };
        Counter counter = new Counter();
        ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        SuperLedgerClient bc = new SuperLedgerClient(new ClientConfiguration(), channelFactory, executor);
        InetSocketAddress addr = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
        int epoch = 1;
        for (int i = 0; i < 100000; i++) {
            counter.inc();
            PayLoad.Builder payload = PayLoad.newBuilder().setEntryId(i).setBody(hello);
            AddDataEntryRequest.Builder addReqBuilder = AddDataEntryRequest.newBuilder()
                    .setAcctName(acctName).setMasterKey(masterKey).addPayloads(payload);
            SimpleRequest.Builder sreqBuilder = SimpleRequest.newBuilder().setAddEntryRequest(addReqBuilder)
                    .setHeader(PacketHeader.newBuilder().setTxnId(i).setType(OperationType.ADD_DATA_ENTRY));
            bc.sendRequest(addr, ledgerName, epoch, acctName.hashCode(), sreqBuilder, cb, counter);
        }
        counter.wait(0);
        System.out.println("Total = " + counter.total());
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }
}
