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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.middleware.Middleware;
import org.apache.bookkeeper.middleware.MiddlewareContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Handle server-side error code to client-side error code conversion
 */
public class ClientProtocolMiddleware implements Middleware {

    private final static Logger LOG = LoggerFactory.getLogger(ClientProtocolMiddleware.class);

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
    }

    @Override
    public void uninitialize() throws IOException {
    }

    @Override
    public Deferred<MiddlewareContext> processRequest(MiddlewareContext ctx) {
        return Deferred.fromResult(ctx);
    }

    @Override
    public Deferred<MiddlewareContext> processResponse(MiddlewareContext ctx) {
        int rc = BKException.Code.IllegalOpException;
        if (ctx.getResponse() instanceof BookieProtocol.BKAddResponse) {
            // convert to BKException code because thats what the uppper
            // layers expect. This is UGLY, there should just be one set of
            // error codes.
            BookieProtocol.BKAddResponse a = (BookieProtocol.BKAddResponse) ctx.getResponse();
            rc = BKException.Code.WriteException;
            switch (a.getErrorCode()) {
            case BookieProtocol.EOK:
                rc = BKException.Code.OK;
                break;
            case BookieProtocol.EBADVERSION:
                rc = BKException.Code.ProtocolVersionException;
                break;
            case BookieProtocol.EFENCED:
                rc = BKException.Code.LedgerFencedException;
                break;
            case BookieProtocol.EUA:
                rc = BKException.Code.UnauthorizedAccessException;
                break;
            case BookieProtocol.EREADONLY:
                rc = BKException.Code.WriteOnReadOnlyBookieException;
                break;
            default:
                LOG.error("Add failed {}", a);
                rc = BKException.Code.WriteException;
                break;
            }
        } else if (ctx.getResponse() instanceof BookieProtocol.BKReadResponse) {
            // convert to BKException code because thats what the uppper
            // layers expect. This is UGLY, there should just be one set of
            // error codes.
            BookieProtocol.BKReadResponse rr = (BookieProtocol.BKReadResponse) ctx.getResponse();
            rc = BKException.Code.ReadException;
            switch (rr.getErrorCode()) {
            case BookieProtocol.EOK:
                rc = BKException.Code.OK;
                break;
            case BookieProtocol.ENOENTRY:
            case BookieProtocol.ENOLEDGER:
                rc = BKException.Code.NoSuchEntryException;
                break;
            case BookieProtocol.EBADVERSION:
                rc = BKException.Code.ProtocolVersionException;
                break;
            case BookieProtocol.EUA:
                rc = BKException.Code.UnauthorizedAccessException;
                break;
            default:
                LOG.error("Read error for {}", rr);
                rc = BKException.Code.ReadException;
                break;
            }
        }
        ctx.getResponse().setErrorCode(rc);
        return Deferred.fromResult(ctx);
    }

}
