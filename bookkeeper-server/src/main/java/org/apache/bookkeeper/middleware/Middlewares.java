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
package org.apache.bookkeeper.middleware;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.bookkeeper.conf.AbstractConfiguration;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class Middlewares extends LinkedList<Middleware> implements Middleware {

    private static final long serialVersionUID = 7100090967089090554L;

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
        for (Middleware mw : this) {
            mw.initialize(conf);
        }
    }

    @Override
    public void uninitialize() throws IOException {
        for (Middleware mw : this) {
            mw.uninitialize();
        }
    }

    @Override
    public Deferred<MiddlewareContext> processRequest(MiddlewareContext ctx) {
        Deferred<MiddlewareContext> result = new Deferred<MiddlewareContext>();
        processRequest(listIterator(), ctx, result);
        return result;
    }

    private Deferred<MiddlewareContext> processRequest(final ListIterator<Middleware> iter, MiddlewareContext ctx,
            final Deferred<MiddlewareContext> result) {
        if (iter.hasNext()) {
            final Middleware middle = iter.next();
            middle.processRequest(ctx).addCallbacks(new Callback<MiddlewareContext, MiddlewareContext>() {
                @Override
                public MiddlewareContext call(MiddlewareContext ctx) throws Exception {
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
    public Deferred<MiddlewareContext> processResponse(MiddlewareContext ctx) {
        Deferred<MiddlewareContext> result = new Deferred<MiddlewareContext>();
        processResponse(listIterator(size()), ctx, result);
        return result;
    }

    private Deferred<MiddlewareContext> processResponse(final ListIterator<Middleware> iter,
            MiddlewareContext ctx, final Deferred<MiddlewareContext> result) {
        if (iter.hasPrevious()) {
            final Middleware middle = iter.previous();
            middle.processResponse(ctx).addCallbacks(new Callback<MiddlewareContext, MiddlewareContext>() {
                @Override
                public MiddlewareContext call(MiddlewareContext ctx) throws Exception {
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
