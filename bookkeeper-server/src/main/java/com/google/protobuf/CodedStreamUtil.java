/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.protobuf;

import java.nio.ByteBuffer;

/**
 * Utils to access protected methods in {@link CodedInputStream}.
 */
public final class CodedStreamUtil {

    private CodedStreamUtil() {}

    /**
     * Create a {@link CodedInputStream} from an immutable array.
     *
     * @param array array buffer
     * @param offset offset of array buffer
     * @param length length of array buffer
     * @return coded input stream
     */
    public static CodedInputStream fromImmutableArray(
            byte[] array, int offset, int length) {
        return CodedInputStream.newInstance(array, offset, length, true);
    }

    /**
     * Create a {@link CodedInputStream} from a nio buffer.
     *
     * @param buffer nio buffer to create the coded input stream.
     * @return coded input stream
     */
    public static CodedInputStream fromNioBuffer(ByteBuffer buffer) {
        return CodedInputStream.newInstance(buffer, true);
    }

    /**
     * Create a {@link CodedOutputStream} with a provided {@link ByteOutput}.
     *
     * @param output byte output
     * @param bufferSize buffer size
     * @return output stream
     */
    public static CodedOutputStream fromByteOutput(ByteOutput output, int bufferSize) {
        return CodedOutputStream.newInstance(output, bufferSize);
    }

    public static boolean hasUnsafeByteBufferOperations() {
        return UnsafeUtil.hasUnsafeByteBufferOperations();
    }

}
