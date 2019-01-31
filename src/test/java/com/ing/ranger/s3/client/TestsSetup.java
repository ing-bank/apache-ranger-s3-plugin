package com.ing.ranger.s3.client;

import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class TestsSetup {
    public Map<String, String> configs;

    @Before
    public void setUp() {
        configs = new HashMap<String, String>();
        configs.put("endpoint", "http://127.0.0.1:8010");
        configs.put("accesskey", "accesskey");
        configs.put("password", "PBEWithMD5AndDES,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,6IxJOOpoFsJXyLNjNf/M9Q==");
    }
}
