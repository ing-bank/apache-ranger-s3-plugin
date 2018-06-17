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

