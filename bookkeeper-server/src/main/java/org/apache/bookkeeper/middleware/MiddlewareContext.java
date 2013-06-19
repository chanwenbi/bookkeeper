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

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.middleware.Requests.Request;
import org.apache.bookkeeper.middleware.Responses.Response;
import org.jboss.netty.channel.Channel;

import com.stumbleupon.async.Callback;

public class MiddlewareContext implements Callback<Exception, Exception> {

    // channel interact with request/response
    protected final Channel channel;
    protected final Map<Object, Object> ctxAttributes;
    protected final Request request;
    protected final Response response;

    public MiddlewareContext(Request request, Response response, Channel channel) {
        this.request = request;
        this.response = response;
        this.channel = channel;
        this.ctxAttributes = new HashMap<Object, Object>();
    }

    public Request getRequest() {
        return request;
    }

    public Response getResponse() {
        return response;
    }

    public Channel getChannel() {
        return this.channel;
    }

    public Object getContextAttribute(Object attr) {
        return ctxAttributes.get(attr);
    }

    public void setContextAttribute(Object attr, Object value) {
        ctxAttributes.put(attr, value);
    }

    @Override
    public Exception call(Exception e) throws Exception {
        channel.write(response);
        return e;
    }

}
