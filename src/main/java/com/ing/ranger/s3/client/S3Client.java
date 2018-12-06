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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.client.BaseClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3Client {
    private String endpoint;
    private String accesskey;
    private String secretkey;
    private String awsregion;

    private static final Log LOG = LogFactory.getLog(S3Client.class);

    private static void logError(String errorMessage) throws Exception {
        LOG.error(errorMessage);
        throw new Exception(errorMessage);
    }

    public S3Client(Map<String, String> configs) throws Exception {
        this.endpoint = configs.get("endpoint");
        this.accesskey = configs.get("accesskey");
        this.secretkey = configs.get("secretkey");
        this.awsregion = RangerConfiguration.getInstance().get("airlock.s3.aws.region", "default");

        if (this.endpoint == null || this.endpoint.isEmpty() || !this.endpoint.startsWith("http")) {
            logError("Incorrect value found for configuration `endpoint`. Please provide url in format http://host:port");
        }
        if (this.accesskey == null || this.secretkey == null) {
            logError("Required value not found. Please provide accesskey, secretkey and user uid");
        }
    }

    private AmazonS3 getAWSClient() {
        AWSCredentials credentials = new BasicAWSCredentials(this.accesskey, this.secretkey);
        // singer type only required util akka http allows Raw User-Agent header
        // airlock changes User-Agent and causes signature mismatch
        ClientConfiguration conf = new ClientConfiguration();
        conf.setSignerOverride("S3SignerType");

        AmazonS3ClientBuilder client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(conf)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, awsregion));

        client.setPathStyleAccessEnabled(true);
        return client.build();
    }

    public Map<String, Object> connectionTest() {
        Map<String, Object> responseData = new HashMap<String, Object>();

        List<Bucket> buckets = getAWSClient().listBuckets();

        if (buckets.get(0).getName().isEmpty()) {
            final String errMessage = "Failed, cannot retrieve Buckets list from S3";
            BaseClient.generateResponseDataMap(false, errMessage, errMessage,
                    null, null, responseData);
        } else {
            final String successMessage = "Connection test successful";
            BaseClient.generateResponseDataMap(true, successMessage, successMessage,
                    null, null, responseData);
        }
        return responseData;
    }

    public List<String> getBuckets(final String userInput) {
        final String needle;
        List<String> buckets = new ArrayList<String>();

        if (userInput != null) {
            needle = userInput;
        } else {
            needle = new String();
        }

        for (Bucket b : getAWSClient().listBuckets()) {
            buckets.add(b.getName());
        }

        List<String> bucketsPaths = buckets
                .stream()
                .filter(b -> b.startsWith(needle))
                .flatMap(b -> getBucketsPseudoDirs(b).stream())
                .collect(Collectors.toList());

        return bucketsPaths;
    }

    public List<String> getBucketsPseudoDirs(final String bucket) {
        List<String> pseudodirs = new ArrayList<String>();

        pseudodirs.add(String.format("/%s", bucket));

        for (S3ObjectSummary o : getAWSClient().listObjects(bucket).getObjectSummaries()) {
            String searchPath = String.format("/%s/%s", bucket, o.getKey());
            pseudodirs.add(searchPath);
        }
        return pseudodirs;
    }
}
