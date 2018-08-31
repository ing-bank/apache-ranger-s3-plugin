package com.ing.ranger.s3.client;

import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class TestsSetup {
    public Map<String, String> configs;

    @Before
    public void setUp() {
        configs = new HashMap<String, String>();
        configs.put("endpoint", "http://127.0.0.1:8010/admin/");
        configs.put("uid", "ceph-admin");
        configs.put("accesskey", "accesskey");
        configs.put("secretkey", "secretkey");
    }
}
