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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.BaseClient;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.User;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class S3Client {
    private String endpoint;
    private String accesskey;
    private String secretkey;
    private String uid; // ceph user uid, used to verify connection to S3 backend

    private static final Log LOG = LogFactory.getLog(S3Client.class);

    private static void logError(String errorMessage) throws Exception {
        LOG.error(errorMessage);
        throw new Exception(errorMessage);
    }

    public S3Client(Map<String, String> configs) throws Exception {
        this.endpoint  = configs.get("endpoint");
        this.accesskey = configs.get("accesskey");
        this.secretkey = configs.get("secretkey");
        this.uid       = configs.get("uid");

        if (this.endpoint == null || this.endpoint.isEmpty() || !this.endpoint.startsWith("http") || !this.endpoint.endsWith("admin")) {
            logError("Incorrect value found for configuration `endpoint`. Please provide url in format http://host:port/admin");
        }
        if (this.accesskey == null || this.secretkey == null || this.uid == null) {
            logError("Required value not found. Please provide accesskey, secretkey and user uid");
        }
    }

    private RgwAdmin getRgwAdmin() {
        return new RgwAdminBuilder()
                .accessKey(this.accesskey)
                .secretKey(this.secretkey)
                .endpoint(this.endpoint)
                .build();
    }

    public List<String> getBuckets(final String userInput) {
        final String needle;
        RgwAdmin rgwAdmin = getRgwAdmin();

        if (userInput != null) {
            needle = userInput;
        } else {
            needle = new String();
        }

        // Empty string ensures returning all buckets
        List<String> bucketNames = rgwAdmin.listBucket("")
                .stream()
                .filter(s -> FilenameUtils.wildcardMatch(s, needle + "*"))
                .collect(Collectors.toList());

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Buckets returned for input=%s buckets=%s", needle, bucketNames));
        }
        return bucketNames;
    }

    public Map<String, Object> connectionTest() {
        Map<String, Object> responseData = new HashMap<String, Object>();
        RgwAdmin rgwAdmin = getRgwAdmin();
        Optional<User> user = rgwAdmin.getUserInfo(this.uid);

        if (!user.isPresent()) {
            final String errMessage = "Failed, cannot retrieve UserInfo for: " + user;
            BaseClient.generateResponseDataMap(false, errMessage, errMessage,
                    null, null, responseData);
        } else {
            final String successMessage = "Connection test successful";
            BaseClient.generateResponseDataMap(true, successMessage, successMessage,
                    null, null, responseData);
        }
        return responseData;
    }
}
