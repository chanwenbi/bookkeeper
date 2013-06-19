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
package org.apache.bookkeeper.security.token;

import static org.apache.bookkeeper.security.token.ClientTokenAuthMiddleware.TOKEN_KEY;

import java.io.IOException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.middleware.Middleware;
import org.apache.bookkeeper.middleware.MiddlewareContext;
import org.apache.bookkeeper.proto.BookieProtocol;

import com.stumbleupon.async.Deferred;

public class ServerTokenAuthMiddleware implements Middleware {

    static class InvalidTokenException extends Exception {
        private static final long serialVersionUID = 5864613645425492331L;

        InvalidTokenException(String token) {
            super("Invalid token : " + token);
        }
    }

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
        // TODO: initialize a token verification manager
    }

    @Override
    public void uninitialize() throws IOException {
    }

    private boolean isValidToken(String token) {
        return true;
    }

    @Override
    public Deferred<MiddlewareContext> processRequest(MiddlewareContext ctx) {
        String tokenValue = ctx.getRequest().getAttribute(TOKEN_KEY);
        if (isValidToken(tokenValue)) {
            return Deferred.fromResult(ctx);
        } else {
            ctx.getResponse().setErrorCode(BookieProtocol.EUA);
            return Deferred.fromError(new InvalidTokenException(tokenValue));
        }
    }

    @Override
    public Deferred<MiddlewareContext> processResponse(MiddlewareContext ctx) {
        return Deferred.fromResult(ctx);
    }

}
