/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.developer.cookbook.flink.events;

import java.time.Instant;

/** Captures the activity of a user within a session. */
public class UserActivity {
    public String user;
    public int numInteractions;
    public Instant activityStart;
    public Instant activityEnd;

    public UserActivity() {}

    public UserActivity(
            final String user,
            final int numInteractions,
            final Instant activityStart,
            final Instant activityEnd) {
        this.user = user;
        this.numInteractions = numInteractions;
        this.activityStart = activityStart;
        this.activityEnd = activityEnd;
    }

    @Override
    public String toString() {
        return String.format(
                "MachineActivity{"
                        + "machine='%-24s'"
                        + ", numInteractions=%-5d"
                        + ", activityStart=%-30s"
                        + ", activityEnd=%-30s"
                        + '}',
                user,
                numInteractions,
                activityStart,
                activityEnd);
    }
}
