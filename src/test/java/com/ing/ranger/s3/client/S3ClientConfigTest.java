package com.ing.ranger.s3.client;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class S3ClientConfigTest {
    private Map<String, String> configs;

    @Before
    public void setUp() {
        configs = new HashMap<String, String>();
        configs.put("endpoint", "http://127.0.0.1:8010/admin");
        configs.put("accesskey", "accesskey");
        configs.put("secretkey", "secretkey");
        configs.put("uid", "ceph-admin");
    }

    @Test(expected = Exception.class)
    public void s3ClientConfEmptyUID() throws Exception {
        configs.remove("uid");
        assertThat(new S3Client(configs));
    }

    @Test(expected = Exception.class)
    public void s3ClientConfEmptyAccessKey() throws Exception {
        configs.remove("accesskey");
        assertThat(new S3Client(configs));
    }

    @Test(expected = Exception.class)
    public void s3ClientConfEmptySecretKey() throws Exception {
        configs.remove("secretkey");
        assertThat(new S3Client(configs));
    }

    @Test(expected = Exception.class)
    public void s3ClientConfEmptyWrongEndPoint() throws Exception {
        configs.remove("endpoint");
        configs.put("endpoint", "http://127.0.0.1:8010");
        assertThat(new S3Client(configs));
    }

}
