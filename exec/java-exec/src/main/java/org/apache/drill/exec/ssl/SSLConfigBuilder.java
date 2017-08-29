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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.ssl;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillConfigurationException;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;


public class SSLConfigBuilder {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(org.apache.drill.exec.ssl.SSLConfigBuilder.class);

  private DrillConfig config = null;
  private Configuration hadoopConfig = null;
  private SSLFactory.Mode mode = SSLFactory.Mode.SERVER;
  private boolean initializeSSLContext = false;
  private boolean validateKeyStore = false;

  public SSLConfigBuilder() {

  }

  public SSLConfig build() throws DrillException {
    if (config == null) {
      throw new DrillConfigurationException(
          "Cannot create SSL configuration from null Drill configuration.");
    }
    SSLConfig sslConfig;
    if (mode == SSLFactory.Mode.SERVER) {
      sslConfig = new SSLConfigServer(config, hadoopConfig);
    } else {
      sslConfig = new SSLConfigClient(config, hadoopConfig);
    }
    if(initializeSSLContext){
      sslConfig.initContext();
    }
    if(validateKeyStore){
      sslConfig.validateKeyStore();
    }
    return sslConfig;
  }

  public SSLConfigBuilder config(DrillConfig config) {
    this.config = config;
    return this;
  }

  public SSLConfigBuilder hadoopConfig(Configuration hadoopConfig) {
    this.hadoopConfig = hadoopConfig;
    return this;
  }

  public SSLConfigBuilder mode(SSLFactory.Mode mode) {
    this.mode = mode;
    return this;
  }

  public SSLConfigBuilder initializeSSLContext(boolean initializeSSLContext) {
    this.initializeSSLContext = initializeSSLContext;
    return this;
  }

  public SSLConfigBuilder validateKeyStore(boolean validateKeyStore) {
    this.validateKeyStore = validateKeyStore;
    return this;
  }
}