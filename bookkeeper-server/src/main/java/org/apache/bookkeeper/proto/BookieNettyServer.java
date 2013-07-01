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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.ssl.SSLContextFactory;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Netty server for serving bookie requests
 */
class BookieNettyServer {
    static Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final static int maxMessageSize = 0xfffff;
    final ServerConfiguration conf;
    final ChannelFactory serverChannelFactory;
    final Bookie bookie;
    final ChannelGroup allChannels = new CleanupChannelGroup();
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    Object suspensionLock = new Object();
    boolean suspended = false;

    final BookieSocketAddress bookieAddr;
    final SSLContextFactory sslContextFactory;

    BookieNettyServer(ServerConfiguration conf, Bookie bookie)
            throws IOException, KeeperException, InterruptedException, BookieException  {
        this.conf = conf;
        this.bookie = bookie;

        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        String base = "bookie-" + conf.getBookiePort() + "-netty";
        serverChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-boss-%d").build()),
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-worker-%d").build()));
        bookieAddr = Bookie.getBookieAddress(conf);
        sslContextFactory = instantiateSSLContextFactory(conf);
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
    }

    private static SSLContextFactory instantiateSSLContextFactory(ServerConfiguration conf) throws IOException {
        Class<? extends SSLContextFactory> sslContextFactoryCls = conf.getSSLContextFactoryClass();
        SSLContextFactory sslContextFactory = null;
        if (null != sslContextFactoryCls) {
            try {
                sslContextFactory = ReflectionUtils.newInstance(sslContextFactoryCls);
            } catch (Throwable t) {
                sslContextFactory = null;
            }
            if (null == sslContextFactory || (null != sslContextFactory && sslContextFactory.isClient())) {
                throw new IOException("Failed to load ssl context factory : " + sslContextFactoryCls);
            }
            if (null != sslContextFactory) {
                sslContextFactory.initialize(conf);
            }
        }
        return sslContextFactory;
    }

    boolean isRunning() {
        return isRunning.get();
    }

    @VisibleForTesting
    void suspendProcessing() {
        synchronized (suspensionLock) {
            suspended = true;
            allChannels.setReadable(false).awaitUninterruptibly();
        }
    }

    @VisibleForTesting
    void resumeProcessing() {
        synchronized (suspensionLock) {
            suspended = false;
            allChannels.setReadable(true).awaitUninterruptibly();
            suspensionLock.notifyAll();
        }
    }

    void start() {
        listenOn(null);
        if (null != sslContextFactory) {
            listenOn(sslContextFactory);
        }
        isRunning.set(true);
    }

    void listenOn(SSLContextFactory sslContextFactory) {
        InetSocketAddress addr;
        if (conf.getListeningInterface() == null) {
            // listening on all interfaces.
            if (null == sslContextFactory) {
                addr = new InetSocketAddress(conf.getBookiePort());
            } else {
                addr = new InetSocketAddress(conf.getBookieSSLPort());
            }
        } else {
            if (null == sslContextFactory) {
                addr = bookieAddr.getSocketAddress();
            } else {
                addr = bookieAddr.getSSLSocketAddress();
            }
        }
        ServerBootstrap bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setPipelineFactory(new BookiePipelineFactory(sslContextFactory));
        bootstrap.setOption("child.tcpNoDelay", conf.getServerTcpNoDelay());
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.soLinger", 2);
        bootstrap.setOption("reuseAddress", true);

        Channel listen = bootstrap.bind(addr);
        allChannels.add(listen);
        LOG.info("Bookie listens on : {}", addr);
    }

    void shutdown() {
        isRunning.set(false);
        allChannels.close().awaitUninterruptibly();
        serverChannelFactory.releaseExternalResources();
    }

    private class BookiePipelineFactory implements ChannelPipelineFactory {
        final SSLContextFactory sslContextFactory;

        BookiePipelineFactory(SSLContextFactory sslContextFactory) {
            this.sslContextFactory = sslContextFactory;
        }

        public ChannelPipeline getPipeline() throws Exception {
            synchronized (suspensionLock) {
                while (suspended) {
                    suspensionLock.wait();
                }
            }
            ChannelPipeline pipeline = Channels.pipeline();
            if (null != sslContextFactory) {
                pipeline.addLast("ssl", new SslHandler(sslContextFactory.getEngine()));
            }
            pipeline.addLast("lengthbaseddecoder",
                             new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

            pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder());
            pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder());
            pipeline.addLast("bookieRequestHandler", new BookieRequestHandler(conf, bookie, allChannels,
                    null != sslContextFactory));
            return pipeline;
        }
    }

    private static class CleanupChannelGroup extends DefaultChannelGroup {
        private AtomicBoolean closed = new AtomicBoolean(false);

        CleanupChannelGroup() {
            super("BookieChannelGroup");
        }

        @Override
        public boolean add(Channel channel) {
            boolean ret = super.add(channel);
            if (closed.get()) {
                channel.close();
            }
            return ret;
        }

        @Override
        public ChannelGroupFuture close() {
            closed.set(true);
            return super.close();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CleanupChannelGroup)) {
                return false;
            }
            CleanupChannelGroup other = (CleanupChannelGroup)o;
            return other.closed.get() == closed.get()
                && super.equals(other);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 17 + (closed.get() ? 1 : 0);
        }
    }
}
