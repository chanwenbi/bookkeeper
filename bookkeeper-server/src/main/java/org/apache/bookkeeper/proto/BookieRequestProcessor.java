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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieRequestProcessor implements RequestProcessor, BookkeeperInternalCallbacks.WriteCallback {

    private final static Logger LOG = LoggerFactory.getLogger(BookieRequestProcessor.class);
    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private ServerConfiguration serverCfg;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    private Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private final ExecutorService readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private final ExecutorService writeThreadPool;

    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;

    public BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie) {
        this.serverCfg = serverCfg;
        this.bookie = bookie;
        this.readThreadPool =
            createExecutor(this.serverCfg.getNumReadWorkerThreads(),
                           "BookieWriteThread-" + serverCfg.getBookiePort() + "-%d");
        this.writeThreadPool =
            createExecutor(this.serverCfg.getNumAddWorkerThreads(),
                           "BookieReadThread-" + serverCfg.getBookiePort() + "-%d");
        this.statsEnabled = serverCfg.isStatisticsEnabled();
    }

    @Override
    public void close() {
        shutdownExecutor(writeThreadPool);
        shutdownExecutor(readThreadPool);
    }

    private ExecutorService createExecutor(int numThreads, String nameFormat) {
        if (numThreads <= 0) {
            return null;
        } else {
            return Executors.newFixedThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
        }
    }

    private void shutdownExecutor(ExecutorService service) {
        if (null != service) {
            service.shutdown();
        }
    }

    @Override
    public void processRequest(Request r, Channel c) {
        BKPacketHeader header = r.getHeader();
        switch (header.getVersion()) {
        case VERSION_THREE:
            break;
        case VERSION_TWO:
            break;
        case VERSION_ONE:
            break;
        case VERSION_ZERO:
            break;
        default:
            LOG.error("Invalid protocol version, expected something between {} & {}. got {}",
                            new Object[] { ProtocolVersion.VERSION_ZERO, ProtocolVersion.VERSION_THREE,
                                            header.getVersion() });
            c.write(ResponseBuilder.buildErrorResponse(StatusCode.EBADVERSION, r));
            return;
        }

        switch (header.getOperation()) {
        case ADD_ENTRY:
            processAddRequest(r, c);
            break;
        case READ_ENTRY:
            processReadRequest(r, c);
            break;
        default:
            LOG.error("Unknown op type {}, sending error", header.getOperation());
            c.write(ResponseBuilder.buildErrorResponse(StatusCode.EBADREQ, r));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
            }
            break;
        }
    }

    class AddCtx {
        final Channel c;
        final Request r;
        final long startTime;

        AddCtx(Channel c, Request r) {
            this.c = c;
            this.r = r;

            if (statsEnabled) {
                startTime = MathUtils.now();
            } else {
                startTime = 0;
            }
        }
    }

    private void processAddRequest(final Request r, final Channel c) {
        if (null == writeThreadPool) {
            handleAdd(r, c);
        } else {
            writeThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    handleAdd(r, c);
                }
            });
        }
    }

    private void handleAdd(Request r, Channel c) {
        AddRequest add = r.getAddRequest();

        if (bookie.isReadOnly()) {
            LOG.warn("BookieServer is running as readonly mode,"
                            + " so rejecting the request from the client!");
            c.write(ResponseBuilder.buildErrorResponse(StatusCode.EREADONLY, r));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
            return;
        }

        StatusCode rc = StatusCode.EOK;
        try {
            if (add.hasFlag() && AddRequest.Flag.RECOVERY_ADD.equals(add.getFlag())) {
                bookie.recoveryAddEntry(add.getBody().asReadOnlyByteBuffer(), this, new AddCtx(c, r),
                                add.getMasterKey().toByteArray());
            } else {
                bookie.addEntry(add.getBody().asReadOnlyByteBuffer(),
                                this, new AddCtx(c, r), add.getMasterKey().toByteArray());
            }
        } catch (IOException e) {
            LOG.error("Error writing " + add, e);
            rc = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException lfe) {
            LOG.error("Attempt to write to fenced ledger", lfe);
            rc = StatusCode.EFENCED;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + add.getLedgerId(), e);
            rc = StatusCode.EUA;
        }
        if (!StatusCode.EOK.equals(rc)) {
            c.write(ResponseBuilder.buildErrorResponse(rc, r));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                    InetSocketAddress addr, Object ctx) {
        assert (ctx instanceof AddCtx);
        AddCtx addctx = (AddCtx) ctx;
        addctx.c.write(ResponseBuilder.buildAddResponse(addctx.r));

        if (statsEnabled) {
            // compute the latency
            if (0 == rc) {
                // for add operations, we compute latency in writeComplete callbacks.
                long elapsedTime = MathUtils.now() - addctx.startTime;
                bkStats.getOpStats(BKStats.STATS_ADD).updateLatency(elapsedTime);
            } else {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    private void processReadRequest(final Request r, final Channel c) {
        if (null == readThreadPool) {
            handleRead(r, c);
        } else {
            readThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    handleRead(r, c);
                }
            });
        }
    }

    private void handleRead(Request r, Channel c) {
        ReadRequest read = r.getReadRequest();

        LOG.debug("Received new read request: {}", r);
        StatusCode errorCode = StatusCode.EIO;
        long startTime = 0;
        if (statsEnabled) {
            startTime = MathUtils.now();
        }
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.hasFlag() && ReadRequest.Flag.FENCE_LEDGER.equals(read.getFlag())) {
                LOG.warn("Ledger " + read.getLedgerId() + " fenced by " + c.getRemoteAddress());

                if (read.hasMasterKey()) {
                    fenceResult = bookie.fenceLedger(read.getLedgerId(), read.getMasterKey().toByteArray());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
                    }
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = bookie.readEntry(read.getLedgerId(), read.getEntryId());
            LOG.debug("##### Read entry ##### {}", data.remaining());
            if (null != fenceResult) {
                // TODO:
                // currently we don't have readCallback to run in separated read
                // threads. after BOOKKEEPER-429 is complete, we could improve
                // following code to make it not wait here
                //
                // For now, since we only try to wait after read entry. so writing
                // to journal and read entry are executed in different thread
                // it would be fine.
                try {
                    Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                    if (null == fenced || !fenced) {
                        // if failed to fence, fail the read request to make it retry.
                        errorCode = StatusCode.EIO;
                        data = null;
                    } else {
                        errorCode = StatusCode.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = StatusCode.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = StatusCode.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = StatusCode.EIO;
                    data = null;
                }
            } else {
                errorCode = StatusCode.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = StatusCode.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = StatusCode.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = StatusCode.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + read.getLedgerId(), e);
            errorCode = StatusCode.EUA;
        }

        LOG.trace("Read entry rc = {} for {}",
                        new Object[] { errorCode, read });
        if (StatusCode.EOK.equals(errorCode)) {
            assert data != null;

            c.write(ResponseBuilder.buildReadResponse(data, r));
            if (statsEnabled) {
                long elapsedTime = MathUtils.now() - startTime;
                bkStats.getOpStats(BKStats.STATS_READ).updateLatency(elapsedTime);
            }
        } else {
            c.write(ResponseBuilder.buildErrorResponse(errorCode, r));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
            }
        }
    }

}
