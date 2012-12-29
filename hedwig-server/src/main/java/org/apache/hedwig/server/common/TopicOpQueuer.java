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
     * Map from topic to the queue of operations for that topic.
     */
    protected ConcurrentHashMap<ByteString, Queue<Runnable>> topic2ops =
        new ConcurrentHashMap<ByteString, Queue<Runnable>>();

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
        final public ByteString topic;
        final public Callback<T> cb;
        final public Object ctx;

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

    protected synchronized void popAndRunNext(ByteString topic) {
        // since we used concurrent hash map, we could
        // get the queue without synchronized whole queue map
        Queue<Runnable> ops = topic2ops.get(topic);
        assert null != ops;
        synchronized (ops) {
            if (!ops.isEmpty())
                ops.remove();
            if (!ops.isEmpty()) {
                scheduler.unsafeSubmitOrdered(topic, ops.peek());
            } else {
                // it's unsafe to remove topic queue here
                // since some other threads may already get ops
                // queue instance, remove it may cause some problems
                // TODO: need to think a better solution for it.
                // topic2ops.remove(topic, ops);
            }
        }
    }

    public void pushAndMaybeRun(ByteString topic, Op op) {
        int size;
        Queue<Runnable> ops = topic2ops.get(topic);
        if (null == ops) {
            Queue<Runnable> newOps = new LinkedList<Runnable>();
            Queue<Runnable> oldOps = topic2ops.putIfAbsent(topic, newOps);
            if (null == oldOps) {
                // no queue associated with the topic
                ops = newOps;
            } else {
                // someone already create a queue
                ops = oldOps;
            }
        }
        synchronized (ops) {
            ops.add(op);
            size = ops.size();
        }
        if (size == 1)
            op.run();
    }

}
