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
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.client.BaseClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3Client {
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String awsRegion;

    private static final Log LOG = LogFactory.getLog(S3Client.class);

    private static void logError(String errorMessage) throws Exception {
        LOG.error(errorMessage);
        throw new Exception(errorMessage);
    }

    public S3Client(Map<String, String> configs) throws Exception {
        this.endpoint = configs.get("endpoint");
        this.accessKey = configs.get("accesskey");
        this.secretKey = configs.get("secretkey");
        this.awsRegion = RangerConfiguration.getInstance().get("airlock.s3.aws.region", "us-east-1");

        if (this.endpoint == null || this.endpoint.isEmpty() || !this.endpoint.startsWith("http")) {
            logError("Incorrect value found for configuration `endpoint`. Please provide url in format http://host:port");
        }
        if (this.accessKey == null || this.secretKey == null) {
            logError("Required value not found. Please provide accessKey, secretKey and user uid");
        }
    }

    private AmazonS3 getAWSClient() {
        AWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
        // singer type only required util akka http allows Raw User-Agent header
        // airlock changes User-Agent and causes signature mismatch
        ClientConfiguration conf = new ClientConfiguration();
        conf.setSignerOverride("S3SignerType");

        AmazonS3ClientBuilder client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(conf)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, awsRegion));

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

    private String removeLeadingSlash(final String userInput) {
       String witoutLeadingSlash;
       if (userInput.startsWith("/")) {
           witoutLeadingSlash = userInput.substring(1);
       } else {
           witoutLeadingSlash = userInput;
       }
       return witoutLeadingSlash;
    }

    public List<String> getBucketPaths(final String userInput) {
        Supplier<Stream<Bucket>> buckets = () -> getAWSClient().listBuckets().stream();
        String[] userInputSplited = removeLeadingSlash(userInput).split("/");
        String bucketFilter = userInputSplited[0];
        String subdirFilter;

        if (userInputSplited.length >= 2) {
            subdirFilter = userInput.substring(removeLeadingSlash(userInput).indexOf("/") + 2);
        } else {
            subdirFilter = "";
        }

        List<String> bucketsPaths = buckets
                .get()
                .filter(b -> b.getName().startsWith(bucketFilter))
                .flatMap(b -> {
                    if (subdirFilter.length() > 0 || userInput.endsWith("/")) {
                      return getBucketsPseudoDirs(b.getName(), subdirFilter).stream();
                    } else {
                        return buckets.get()
                                .filter(sb->sb.getName().startsWith(bucketFilter))
                                .map(sb-> String.format("/%s",sb.getName()));
                    }
                })
                .distinct()
                .sorted()
                .limit(50)
                .collect(Collectors.toList());

        return bucketsPaths;
    }

    public List<String> getBucketsPseudoDirs(final String bucket, final String subdirFilter) {
        ObjectListing bucketObjects = getAWSClient().listObjects(bucket);

        List<String> pseduDirsFiltered = bucketObjects
                .getObjectSummaries()
                .stream()
                .filter(p -> {
                    if(subdirFilter.length() > 0) {
                        return p.getKey().startsWith(subdirFilter);
                    } else {
                        return true;
                    }
                })
                .map(p -> {
                    if (p.getSize() == 0) {
                        return String.format("/%s/%s", bucket, p.getKey());
                    } else {
                        Integer endIndex = p.getKey().contains("/") ? p.getKey().lastIndexOf("/") : 0;
                        if (endIndex > 0) {
                            return String.format("/%s/%s/", bucket, p.getKey().substring(0, endIndex));
                        } else {
                            return String.format("/%s/", bucket);
                        }

                    }
                })
                .collect(Collectors.toList());

        return pseduDirsFiltered;
    }
}
