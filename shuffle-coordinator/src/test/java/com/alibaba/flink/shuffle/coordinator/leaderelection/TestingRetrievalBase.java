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

package com.alibaba.flink.shuffle.coordinator.leaderelection;

import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalListener;
import com.alibaba.flink.shuffle.coordinator.utils.CommonTestUtils;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base class which provides some convenience functions for testing purposes of {@link
 * LeaderRetrievalListener}.
 */
public class TestingRetrievalBase {

    private final BlockingQueue<LeaderInformation> leaderEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    private LeaderInformation leader = LeaderInformation.empty();
    private String oldAddress;
    private Throwable error;

    public String waitForNewLeader(long timeout) throws Exception {
        throwExceptionIfNotNull();

        final String errorMsg =
                "Listener was not notified about a new leader within " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    leader = leaderEventQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return leader != null
                            && !leader.isEmpty()
                            && !leader.getLeaderAddress().equals(oldAddress);
                },
                timeout,
                errorMsg);

        oldAddress = leader.getLeaderAddress();

        return leader.getLeaderAddress();
    }

    public void waitForEmptyLeaderInformation(long timeout) throws Exception {
        throwExceptionIfNotNull();

        final String errorMsg =
                "Listener was not notified about an empty leader within " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    leader = leaderEventQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return leader != null && leader.isEmpty();
                },
                timeout,
                errorMsg);

        oldAddress = null;
    }

    public void waitForError(long timeout) throws Exception {
        final String errorMsg = "Listener did not see an exception with " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    error = errorQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return error != null;
                },
                timeout,
                errorMsg);
    }

    public void handleError(Throwable ex) {
        errorQueue.offer(ex);
    }

    public LeaderInformation getLeader() {
        return leader;
    }

    public String getAddress() {
        return leader.getLeaderAddress();
    }

    public UUID getLeaderSessionID() {
        return leader.getLeaderSessionID();
    }

    public void offerToLeaderQueue(LeaderInformation leaderInfo) {
        leaderEventQueue.offer(leaderInfo);
        this.leader = leaderInfo;
    }

    public int getLeaderEventQueueSize() {
        return leaderEventQueue.size();
    }

    /**
     * Please use {@link #waitForError} before get the error.
     *
     * @return the error has been handled.
     */
    @Nullable
    public Throwable getError() {
        return this.error;
    }

    private void throwExceptionIfNotNull() throws Exception {
        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }
}
