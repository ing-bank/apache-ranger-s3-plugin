package com.ing.ranger.s3;

import com.ing.ranger.s3.client.S3ResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceS3 extends RangerBaseService {

  private static final Log LOG = LogFactory.getLog(RangerServiceS3.class);

  @Override
  public Map<String, Object> validateConfig() throws Exception {
    Map<String, Object> ret = new HashMap<String, Object>();
    String serviceName = getServiceName();

    if (configs != null) {
      ret = S3ResourceManager.validateConfig(serviceName, configs);
    }

    return ret;
  }

  @Override
  public List<String> lookupResource(ResourceLookupContext context) throws Exception {
    List<String> ret = new ArrayList<String>();
    if (context != null) {
      ret = S3ResourceManager.getBuckets(getServiceName(), getConfigs(), context);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RangerServiceS3.lookupResource Response: (" + ret + ")");
    }

    return ret;
  }


}
