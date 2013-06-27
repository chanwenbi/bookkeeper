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

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

public class Requests {

    public static interface Request {

        public String getAttribute(String attr);

        public void setAttribute(String attr, String value);
    }

    public static interface LedgerRequest extends Request {

        public long getLedgerId();

        public void setLedgerId(long ledgerId);

        public long getEntryId();

        public void setEntryId(long entryId);

        public boolean hasMasterKey();

        public byte[] getMasterKey();

        public void setMasterKey(byte[] masterKey);

    }

    public static interface AddRequest extends LedgerRequest {

        public ChannelBuffer getDataAsChannelBuffer();

        public ByteBuffer getDataAsByteBuffer();

        public void setData(ChannelBuffer data);

        public boolean isRecoveryAdd();

        public void setRecoveryAdd();
    }

    public static interface ReadRequest extends LedgerRequest {

        public boolean isFencingRequest();

        public void enableFencing();
    }

}
