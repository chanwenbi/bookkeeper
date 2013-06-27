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

import java.io.IOException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.processor.ClientRequestProcessor;
import org.apache.bookkeeper.processor.RequestContext;
import org.apache.bookkeeper.processor.ResponseContext;

import com.stumbleupon.async.Deferred;

public class ClientTokenAuthProcessor implements ClientRequestProcessor {

    public static final String TOKEN_KEY = "authentication";

    private String tokenValue = "";

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
        // do initialize, maybe obtain its token
        tokenValue = "token=secrettoken";
    }

    @Override
    public void uninitialize() throws IOException {
    }

    @Override
    public Deferred<RequestContext> processRequest(RequestContext ctx) {
        // inject the token fields in the request header
        ctx.getRequest().setAttribute(TOKEN_KEY, tokenValue);
        return Deferred.fromResult(ctx);
    }

    @Override
    public Deferred<ResponseContext> processResponse(ResponseContext ctx) {
        return Deferred.fromResult(ctx);
    }

}
