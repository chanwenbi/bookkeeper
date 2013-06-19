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
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.middleware.Middleware;
import org.apache.bookkeeper.middleware.MiddlewareContext;
import org.apache.bookkeeper.middleware.Requests.AddRequest;
import org.apache.bookkeeper.middleware.Requests.ReadRequest;
import org.apache.bookkeeper.middleware.Requests.Request;
import org.apache.bookkeeper.middleware.Responses.ReadResponse;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

public class BookieMiddleware implements Middleware, BookkeeperInternalCallbacks.WriteCallback {

    private final static Logger LOG = LoggerFactory.getLogger(BookieMiddleware.class);

    private final Bookie bookie;

    public BookieMiddleware(Bookie bookie) {
        this.bookie = bookie;
    }

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
    }

    @Override
    public void uninitialize() throws IOException {
        // nope
    }

    @Override
    public Deferred<MiddlewareContext> processRequest(MiddlewareContext ctx) {
        Deferred<MiddlewareContext> result = new Deferred<MiddlewareContext>();
        Request request = ctx.getRequest();
        if (request instanceof AddRequest) {
            handleAdd((AddRequest) request, ctx, result);
        } else if (request instanceof ReadRequest) {
            handleRead((ReadRequest) request, ctx, result);
        } else {
            ctx.getResponse().setErrorCode(BookieProtocol.EBADREQ);
            result.callback(ctx);
        }
        return result;
    }

    private void handleAdd(AddRequest add, MiddlewareContext ctx, Deferred<MiddlewareContext> result) {
        if (bookie.isReadOnly()) {
            LOG.warn("BookieServer is running as readonly mode," + " so rejecting the request from the client!");
            ctx.getResponse().setErrorCode(BookieProtocol.EREADONLY);
            result.callback(ctx);
            return;
        }

        int rc = BookieProtocol.EOK;
        try {
            if (add.isRecoveryAdd()) {
                bookie.recoveryAddEntry(add.getDataAsByteBuffer(), this, new AddCtx(ctx, result), add.getMasterKey());
            } else {
                bookie.addEntry(add.getDataAsByteBuffer(), this, new AddCtx(ctx, result), add.getMasterKey());
            }
        } catch (IOException e) {
            LOG.error("Error writing {} : ", add, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException lfe) {
            LOG.error("Attempt to write to fenced ledger : ", lfe);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger {} : ", add.getLedgerId(), e);
            rc = BookieProtocol.EUA;
        }
        if (rc != BookieProtocol.EOK) {
            ctx.getResponse().setErrorCode(rc);
            result.callback(ctx);
        }
    }

    private void handleRead(ReadRequest read, MiddlewareContext ctx, Deferred<MiddlewareContext> result) {
        LOG.debug("Received new read request: {}", read);
        int errorCode = BookieProtocol.EIO;
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.isFencingRequest()) {
                LOG.warn("Ledger " + read.getLedgerId() + " fenced by " + ctx.getChannel().getRemoteAddress());
                if (read.hasMasterKey()) {
                    fenceResult = bookie.fenceLedger(read.getLedgerId(), read.getMasterKey());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
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
                        errorCode = BookieProtocol.EIO;
                        data = null;
                    } else {
                        errorCode = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                }
            } else {
                errorCode = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + read.getLedgerId(), e);
            errorCode = BookieProtocol.EUA;
        }

        LOG.trace("Read entry rc = {} for {}", errorCode, read);
        ReadResponse rr = (ReadResponse) ctx.getResponse();
        rr.setErrorCode(errorCode);
        if (errorCode == BookieProtocol.EOK) {
            assert data != null;
            rr.setData(ChannelBuffers.wrappedBuffer(data));
        }
        result.callback(ctx);
    }

    @Override
    public Deferred<MiddlewareContext> processResponse(MiddlewareContext ctx) {
        // nope
        return Deferred.fromResult(ctx);
    }

    class AddCtx {
        final MiddlewareContext ctx;
        final Deferred<MiddlewareContext> result;

        AddCtx(MiddlewareContext ctx, Deferred<MiddlewareContext> result) {
            this.ctx = ctx;
            this.result = result;
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
        assert (ctx instanceof AddCtx);
        AddCtx addctx = (AddCtx) ctx;
        // mark response as done
        addctx.ctx.getResponse().setErrorCode(BookieProtocol.EOK);
        addctx.result.callback(addctx.ctx);
    }

}
