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
package org.apache.bookkeeper.processor;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.bookkeeper.conf.AbstractConfiguration;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class ChainRequestProcessor<ReqCtx extends ProcessorContext, RespCtx extends ProcessorContext> extends
        LinkedList<RequestProcessor<ReqCtx, RespCtx>> implements RequestProcessor<ReqCtx, RespCtx> {

    private static final long serialVersionUID = 7100090967089090554L;

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
        for (RequestProcessor<ReqCtx, RespCtx> rp : this) {
            rp.initialize(conf);
        }
    }

    @Override
    public void uninitialize() throws IOException {
        for (RequestProcessor<ReqCtx, RespCtx> rp : this) {
            rp.uninitialize();
        }
    }

    @Override
    public Deferred<ReqCtx> processRequest(ReqCtx ctx) {
        Deferred<ReqCtx> result = new Deferred<ReqCtx>();
        processRequest(listIterator(), ctx, result);
        return result;
    }

    private Deferred<ReqCtx> processRequest(final ListIterator<RequestProcessor<ReqCtx, RespCtx>> iter, ReqCtx ctx,
            final Deferred<ReqCtx> result) {
        if (iter.hasNext()) {
            final RequestProcessor<ReqCtx, RespCtx> processor = iter.next();
            processor.processRequest(ctx).addCallbacks(new Callback<ReqCtx, ReqCtx>() {
                @Override
                public ReqCtx call(ReqCtx ctx) throws Exception {
                    processRequest(iter, ctx, result);
                    return ctx;
                }
            }, new Callback<Exception, Exception>() {
                @Override
                public Exception call(Exception exception) throws Exception {
                    // stop processing when exception is thrown
                    result.callback(exception);
                    return exception;
                }
            });
            return result;
        } else {
            result.callback(ctx);
            return result;
        }
    }

    @Override
    public Deferred<RespCtx> processResponse(RespCtx ctx) {
        Deferred<RespCtx> result = new Deferred<RespCtx>();
        processResponse(listIterator(size()), ctx, result);
        return result;
    }

    private Deferred<RespCtx> processResponse(final ListIterator<RequestProcessor<ReqCtx, RespCtx>> iter, RespCtx ctx,
            final Deferred<RespCtx> result) {
        if (iter.hasPrevious()) {
            final RequestProcessor<ReqCtx, RespCtx> processor = iter.previous();
            processor.processResponse(ctx).addCallbacks(new Callback<RespCtx, RespCtx>() {
                @Override
                public RespCtx call(RespCtx ctx) throws Exception {
                    processResponse(iter, ctx, result);
                    return ctx;
                }
            }, new Callback<Exception, Exception>() {
                @Override
                public Exception call(Exception exception) throws Exception {
                    // stop processing when exception is thrown
                    result.callback(exception);
                    return exception;
                }

            });
            return result;
        } else {
            result.callback(ctx);
            return result;
        }
    }

}
