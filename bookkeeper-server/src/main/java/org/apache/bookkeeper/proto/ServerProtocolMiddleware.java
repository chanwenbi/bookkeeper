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

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.middleware.Middleware;
import org.apache.bookkeeper.middleware.MiddlewareContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

public class ServerProtocolMiddleware implements Middleware {

    private final static Logger LOG = LoggerFactory.getLogger(ServerProtocolMiddleware.class);

    public static class BadVersionException extends Exception {

        private static final long serialVersionUID = 3593554905863300712L;

        public BadVersionException(byte version) {
            super("Invalid protocol version : " + version);
        }

    }

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
    }

    @Override
    public void uninitialize() throws IOException {
    }

    @Override
    public Deferred<MiddlewareContext> processRequest(MiddlewareContext ctx) {
        if (!(ctx.getRequest() instanceof BookieProtocol.BKLedgerRequest)) {
            return Deferred.fromResult(ctx);
        }
        BookieProtocol.BKLedgerRequest r = (BookieProtocol.BKLedgerRequest) ctx.getRequest();
        if (r.getProtocolVersion() < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                || r.getProtocolVersion() > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            LOG.error("Invalid protocol version, expected something between "
                    + BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION + " & " + BookieProtocol.CURRENT_PROTOCOL_VERSION
                    + ". got " + r.getProtocolVersion());
            ctx.getResponse().setErrorCode(BookieProtocol.EBADVERSION);
            return Deferred.fromError(new BadVersionException(r.getProtocolVersion()));
        }
        return Deferred.fromResult(ctx);
    }

    @Override
    public Deferred<MiddlewareContext> processResponse(MiddlewareContext ctx) {
        return Deferred.fromResult(ctx);
    }

}
