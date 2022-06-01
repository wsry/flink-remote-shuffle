/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.coordinator.heartbeat;

import com.alibaba.flink.shuffle.core.ids.InstanceID;

import org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * The default implementation of {@link HeartbeatMonitor}.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public class HeartbeatMonitorImpl<O> implements HeartbeatMonitor<O>, Runnable {

    /** Resource ID of the monitored heartbeat target. */
    private final InstanceID instanceID;

    /** Associated heartbeat target. */
    private final HeartbeatTarget<O> heartbeatTarget;

    private final ScheduledExecutorService scheduledExecutor;

    /** Listener which is notified about heartbeat timeouts. */
    private final HeartbeatListener<?, ?> heartbeatListener;

    /** Maximum heartbeat timeout interval. */
    private final long heartbeatTimeoutIntervalMs;

    private volatile ScheduledFuture<?> futureTimeout;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    protected final Logger log;

    private volatile long lastHeartbeat;

    HeartbeatMonitorImpl(
            InstanceID instanceID,
            HeartbeatTarget<O> heartbeatTarget,
            ScheduledExecutorService scheduledExecutor,
            HeartbeatListener<?, O> heartbeatListener,
            long heartbeatTimeoutIntervalMs,
            Logger log) {

        this.instanceID = checkNotNull(instanceID);
        this.heartbeatTarget = checkNotNull(heartbeatTarget);
        this.scheduledExecutor = checkNotNull(scheduledExecutor);
        this.heartbeatListener = checkNotNull(heartbeatListener);
        this.log = checkNotNull(log);

        checkArgument(
                heartbeatTimeoutIntervalMs > 0L,
                "The heartbeat timeout interval has to be larger than 0.");
        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public HeartbeatTarget<O> getHeartbeatTarget() {
        return heartbeatTarget;
    }

    @Override
    public InstanceID getHeartbeatTargetId() {
        return instanceID;
    }

    @Override
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    @Override
    public void reportHeartbeat() {
        lastHeartbeat = System.currentTimeMillis();
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public void cancel() {
        // we can only cancel if we are in state running
        if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
            cancelTimeout();
        }
    }

    @Override
    public void run() {
        try {
            // The heartbeat has timed out if we're in state running
            if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
                heartbeatListener.notifyHeartbeatTimeout(instanceID);
            }
        } catch (Throwable throwable) {
            log.error("Failed to notify heartbeat timeout.", throwable);
        }
    }

    void resetHeartbeatTimeout(long heartbeatTimeout) {
        if (state.get() == State.RUNNING) {
            cancelTimeout();

            futureTimeout =
                    scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

            // Double check for concurrent accesses (e.g. a firing of the scheduled future)
            if (state.get() != State.RUNNING) {
                cancelTimeout();
            }
        }
    }

    private void cancelTimeout() {
        if (futureTimeout != null) {
            futureTimeout.cancel(true);
        }
    }

    private enum State {
        RUNNING,
        TIMEOUT,
        CANCELED
    }

    /**
     * The factory that instantiates {@link HeartbeatMonitorImpl}.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class Factory<O> implements HeartbeatMonitor.Factory<O> {

        @Override
        public HeartbeatMonitor<O> createHeartbeatMonitor(
                InstanceID instanceID,
                HeartbeatTarget<O> heartbeatTarget,
                ScheduledExecutorService scheduledExecutor,
                HeartbeatListener<?, O> heartbeatListener,
                long heartbeatTimeoutIntervalMs,
                Logger log) {

            return new HeartbeatMonitorImpl<>(
                    instanceID,
                    heartbeatTarget,
                    scheduledExecutor,
                    heartbeatListener,
                    heartbeatTimeoutIntervalMs,
                    log);
        }
    }
}
