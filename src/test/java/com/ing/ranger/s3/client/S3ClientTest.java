/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ing.ranger.s3.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class S3ClientTest extends TestsSetup {

  @Test
  public void testGetBuckets() throws Exception {
    S3Client client = new S3Client(configs);
    assertNotNull(client.getBuckets("demo"));
    assertThat(client.getBuckets("demobucket/")).contains("/demobucket/subdir1/");
    assertThat(client.getBuckets("demobucket/")).contains("/demobucket/subdir2/");
  }

  @Test
  public void testConnectionTest() throws Exception {
    S3Client client = new S3Client(configs);
    assertThat(client.connectionTest().get("connectivityStatus")).isEqualTo(true);
  }

  @Test(expected = Exception.class)
  public void connectionFailure() throws Exception {
    configs.remove("endpoint");
    configs.put("endpoint", "http://ceph:8080");
    S3Client client = new S3Client(configs);
    assertThat(client.connectionTest().get("connectivityStatus")).isEqualTo(false);
  }
}
