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
package org.apache.bookkeeper.stats;

import java.io.IOException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.processor.Requests.AddRequest;
import org.apache.bookkeeper.processor.Requests.ReadRequest;
import org.apache.bookkeeper.processor.Requests.Request;
import org.apache.bookkeeper.processor.ServerProcessorContext;
import org.apache.bookkeeper.processor.ServerRequestProcessor;
import org.apache.bookkeeper.proto.BKStats;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.MathUtils;

import com.stumbleupon.async.Deferred;

public class StatsProcessor implements ServerRequestProcessor {

    private final static Object OP_TIME_ATTR = new Object();

    private final BKStats bkStats = BKStats.getInstance();

    @Override
    public void initialize(AbstractConfiguration conf) throws IOException {
    }

    @Override
    public void uninitialize() throws IOException {
    }

    @Override
    public Deferred<ServerProcessorContext> processRequest(ServerProcessorContext ctx) {
        ctx.setContextAttribute(OP_TIME_ATTR, MathUtils.now());
        return Deferred.fromResult(ctx);
    }

    @Override
    public Deferred<ServerProcessorContext> processResponse(ServerProcessorContext ctx) {
        int statsType = requestType2StatsType(ctx.getRequest());
        int errorCode = ctx.getResponse().getErrorCode();
        if (BookieProtocol.EOK == errorCode) {
            long startTs = (Long) ctx.getContextAttribute(OP_TIME_ATTR);
            bkStats.getOpStats(statsType).updateLatency(MathUtils.now() - startTs);
        } else {
            bkStats.getOpStats(statsType).incrementFailedOps();
        }
        return Deferred.fromResult(ctx);
    }

    static int requestType2StatsType(Request r) {
        if (r instanceof AddRequest) {
            return BKStats.STATS_ADD;
        } else if (r instanceof ReadRequest) {
            return BKStats.STATS_READ;
        } else {
            return BKStats.STATS_UNKNOWN;
        }
    }

}
