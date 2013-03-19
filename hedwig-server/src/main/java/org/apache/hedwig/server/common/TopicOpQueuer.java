/**
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
package org.apache.hedwig.server.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.util.Callback;

public class TopicOpQueuer {

    /**
     * A Placeholder for a queue of topic operations.
     */
    public static class TopicQueue {
        // flag indicating that the queue is removed
        public boolean removed = false;
        // Queue of items
        public final Queue<Runnable> ops = new LinkedList<Runnable>();
    }

    /**
     * Map from topic to the queue of operations for that topic.
     */
    protected ConcurrentHashMap<ByteString, TopicQueue> topic2ops =
        new ConcurrentHashMap<ByteString, TopicQueue>();

    protected final OrderedSafeExecutor scheduler;

    public TopicOpQueuer(OrderedSafeExecutor scheduler) {
        this.scheduler = scheduler;
    }

    // We should not use SafeRunnable for Hedwig here
    // since SafeRunnable will caught and ignore all potention throwable
    // which is bad. Check 'TestPubSubServer' test cases for the reason.
    public interface Op extends Runnable {
    }

    public abstract class AsynchronousOp<T> implements Op {
        public ByteString topic;
        public Callback<T> cb;
        public Object ctx;

        public AsynchronousOp(final ByteString topic, final Callback<T> cb, Object ctx) {
            this.topic = topic;
            this.cb = new Callback<T>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, exception);
                    popAndRunNext(topic);
                }

                @Override
                public void operationFinished(Object ctx, T resultOfOperation) {
                    cb.operationFinished(ctx, resultOfOperation);
                    popAndRunNext(topic);
                }
            };
            this.ctx = ctx;
        }
    }

    public abstract class SynchronousOp implements Op {
        final public ByteString topic;

        public SynchronousOp(ByteString topic) {
            this.topic = topic;
        }

        @Override
        public final void run() {
            runInternal();
            popAndRunNext(topic);
        }

        protected abstract void runInternal();

    }

    public void popAndRunNext(ByteString topic) {
        // since we used concurrent hash map, we could
        // get the queue without synchronized whole queue map
        TopicQueue queue = topic2ops.get(topic);
        assert null != queue;
        synchronized (queue) {
            assert !queue.removed;
            if (!queue.ops.isEmpty())
                queue.ops.remove();
            if (!queue.ops.isEmpty()) {
                scheduler.unsafeSubmitOrdered(topic, queue.ops.peek());
            } else {
                // remove the topic queue here and mark it as removed.
                queue.removed = true;
                topic2ops.remove(topic, queue);
            }
        }
    }

    public void pushAndMaybeRun(ByteString topic, Op op) {
        int size;
        boolean done = false;
        while (!done) {
            TopicQueue queue = topic2ops.get(topic);
            if (null == queue) {
                TopicQueue newQueue = new TopicQueue();
                TopicQueue oldQueue = topic2ops.putIfAbsent(topic, newQueue);
                if (null == oldQueue) {
                    // no queue associated with the topic
                    queue = newQueue;
                } else {
                    // someone already put the queue
                    queue = oldQueue;
                }
            }
            synchronized (queue) {
                // check removed flag in case some one removed the queue at the time it waits
                // for the lock of this queue.
                if (!queue.removed) {
                    queue.ops.add(op);
                    size = queue.ops.size();
                    done = true;
                } else {
                    continue;
                }
            }
            if (size == 1)
                op.run();
        }
    }

}
