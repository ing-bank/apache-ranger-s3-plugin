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

package com.ing.ranger.s3;

import com.ing.ranger.s3.client.S3ResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceS3 extends RangerBaseService {

  private static final Log LOG = LogFactory.getLog(RangerServiceS3.class);

  // check if init is requred
  @Override
  public void init(RangerServiceDef serviceDef, RangerService service) {
      super.init(serviceDef, service);
  }

  @Override
  public Map<String, Object> validateConfig() throws Exception {
    Map<String, Object> ret = new HashMap<String, Object>();
    String serviceName = getServiceName();

    if(LOG.isDebugEnabled()){
      LOG.debug("RangerServiceS3.validateConfig(): Service: " +
      serviceName);
    }

    if (configs != null) {
      try {
        ret = S3ResourceManager.validateConfig(serviceName, configs);
      } catch (Exception e) {
        LOG.error("RangerServiceS3.validateConfig(): Error: ", e);
        throw e;
      }
    }

    if(LOG.isDebugEnabled()){
      LOG.debug("RangerServiceS3.validateConfig(): Response: " +
              ret);
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
      LOG.debug("RangerServiceS3.lookupResource() Response: " +
              ret);
    }
    return ret;
  }


}
