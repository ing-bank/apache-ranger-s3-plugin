package com.ing.ranger.s3.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class S3ClientTest {
  private Map<String, String> configs;
  private String serviceName;

  @Before
  public void setUp() throws Exception {
    configs = new HashMap<String, String>();

    configs.put("endpoint", "http://127.0.0.1:10080/admin/");
    configs.put("uid", "ceph-admin");
    configs.put("accesskey", "accesskey");
    configs.put("secretkey", "secretkey");

  }

  @Test
  public void testGetBuckets() {
    S3Client client = new S3Client(configs);

    assertThat(client.getBuckets(null)).isNotNull();
  }

  @Test
  public void testConnectionTest() {
    S3Client client = new S3Client(configs);

    Map<String, Object> response = client.connectionTest();
    assertThat(response.get("connectivityStatus"));
  }
}
