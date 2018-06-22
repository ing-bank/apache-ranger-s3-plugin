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

import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.List;
import java.util.Map;

public class S3ResourceManager {
  public static Map<String, Object> validateConfig(
    String serviceName, Map<String, String> configs) throws Exception {
    Map<String, Object> ret = null;

    try {
      final S3Client client = new S3Client(configs);
      if (client != null) {
        synchronized (client) {
          ret = client.connectionTest();
        }
      }
    } catch (Exception e) {
      throw e;
    }

    return ret;
  }

  public static List<String> getBuckets(String serviceName, Map<String, String> configs,
                                        ResourceLookupContext context) {
    String userInput = context.getUserInput();
    List<String> results = null;

    if (configs == null || configs.isEmpty()) {
      // log
    } else {
      final S3Client client = new S3Client(configs);
      if (client != null) {
        synchronized (client) {
          results = client.getBuckets(userInput);
        }
      }
    }

    return results;
  }
}

