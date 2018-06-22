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

/*
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
*/
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
  private String uid;

  private static final Log LOG = LogFactory.getLog(S3Client.class);

  public S3Client(Map<String, String> configs) {
    this.endpoint = configs.get("endpoint");
    this.accesskey = configs.get("accesskey");
    this.secretkey = configs.get("secretkey");
    this.uid = configs.get("uid");

    if (this.endpoint == null || this.endpoint.isEmpty()) {
      LOG.error("No value found for configuration `endpoint`. Lookup will fail");
    }

    if (this.accesskey == null || this.accesskey.isEmpty()) {
      LOG.error("No value found for configuration `key`. Lookup will fail");
    }

    if (this.secretkey == null || this.secretkey.isEmpty()) {
      LOG.error("No value found for configuration `token`. Lookup will fail");
    }

    if (this.uid == null || this.uid.isEmpty()) {
      LOG.error("No value found for configuration `token`. Lookup will fail");
    }
  }

  /*protected static AmazonS3 createS3(String accesskey, String secretkey, String endpoint) {
    AWSCredentials credentials = new BasicAWSCredentials(accesskey, secretkey);

    AmazonS3 s3 = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
      .build();

    return s3;
  }*/

  public List<String> getBuckets(final String userInput) {
    final String needle;

    /*AmazonS3 s3 = createS3(this.accesskey, this.secretkey, this.endpoint);

    List<Bucket> buckets = s3.listBuckets();
    List<String> bucket_names = buckets.stream()
      .map(bucket -> bucket.getName())
      .collect(Collectors.toList());

    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Buckets returned: %s", bucket_names));
    }
    */
    RgwAdmin rgwAdmin = new RgwAdminBuilder()
      .accessKey(this.accesskey)
      .secretKey(this.secretkey)
      .endpoint(this.endpoint)
      .build();

    if (userInput != null) {
      needle = userInput;
    } else {
      needle = new String();
    }

    // Empty string ensures returning all buckets
    List<String> bucket_names = rgwAdmin.listBucket("")
      .stream()
      .filter(s -> FilenameUtils.wildcardMatch(s, needle + "*"))
      .collect(Collectors.toList());

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Buckets returned for input=%s buckets=%s", needle, bucket_names));
    }

    return bucket_names;
  }

  public Map<String, Object> connectionTest() {
    Map<String, Object> responseData = new HashMap<String, Object>();

    RgwAdmin rgwAdmin = new RgwAdminBuilder()
      .accessKey(this.accesskey)
      .secretKey(this.secretkey)
      .endpoint(this.endpoint)
      .build();

    Optional<User> user = rgwAdmin.getUserInfo(this.uid);

    if (!user.isPresent()) {
      final String errMessage = "Cannot connect to S3 endpoint (or radosgw)";
      BaseClient.generateResponseDataMap(false, errMessage, errMessage,
        null, null, responseData);
    } else {
      final String successMessage = "Connection test successful";
      BaseClient.generateResponseDataMap(true, successMessage, successMessage,
        null, null, responseData);
    }

    return responseData;
  }

  protected void login() {
    // NOOP: This is not Hadoop
  }
}
