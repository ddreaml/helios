/*
 * Copyright (c) 2014 Spotify AB.
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
 */

package com.spotify.helios.system;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LabelTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost(), "--label", "role=foo", "--label", "some_label");
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the agent to report labels
    final List<String> labels = Polling.await(LONG_WAIT_SECONDS, SECONDS,
        new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        Map<String, HostStatus> status = Json.read(
            cli("hosts", testHost(), "--json"),
            new TypeReference<Map<String, HostStatus>>() {});
        final List<String> labels = status.get(testHost()).getLabels();
        if (labels != null && !labels.isEmpty()) {
          return labels;
        } else {
          return null;
        }
      }
    });

    assertEquals(ImmutableList.of("role=foo", "some_label"), labels);

    assertThat(cli("hosts", "--label", "role=foo"), containsString(testHost()));
    assertThat(cli("hosts", "--label", "some_label"), containsString(testHost()));
    assertThat(cli("hosts", "--label", "non-existent-label"), not(containsString(testHost())));
  }
}
