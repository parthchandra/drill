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

import com.google.common.base.Preconditions;
import io.netty.handler.ssl.SslContext;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.text.MessageFormat;

public abstract class SSLConfig {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfig.class);

  public static final String DEFAULT_SSL_PROVIDER = "JDK"; // JDK or OPENSSL
  public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
  public static final int DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS = 10 * 1000; // 10 seconds

  protected final boolean httpsEnabled;
  protected final DrillConfig config;
  protected final Configuration hadoopConfig;
  protected SslContext sslContext;

  public static final String HADOOP_SSL_CONF_TPL_KEY = "hadoop.ssl.{0}.conf";
  public static final String HADOOP_SSL_KEYSTORE_LOCATION_TPL_KEY = "ssl.{0}.keystore.location";
  public static final String HADOOP_SSL_KEYSTORE_PASSWORD_TPL_KEY = "ssl.{0}.keystore.password";
  public static final String HADOOP_SSL_KEYSTORE_TYPE_TPL_KEY = "ssl.{0}.keystore.type";
  public static final String HADOOP_SSL_KEYSTORE_KEYPASSWORD_TPL_KEY =
      "ssl.{0}.keystore.keypassword";
  public static final String HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY = "ssl.{0}.truststore.location";
  public static final String HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY = "ssl.{0}.truststore.password";
  public static final String HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY = "ssl.{0}.truststore.type";

  public SSLConfig(DrillConfig config, Configuration hadoopConfig, SSLFactory.Mode mode)
      throws DrillException {

    this.config = config;
    httpsEnabled =
        config.hasPath(ExecConstants.HTTP_ENABLE_SSL) && config.getBoolean(ExecConstants.HTTP_ENABLE_SSL);
    // For testing we will mock up a hadoop configuration, however for regular use, we find the actual hadoop config.
    boolean enableHadoopConfig = config.getBoolean(ExecConstants.SSL_USE_HADOOP_CONF);
    if (enableHadoopConfig) {
      if (hadoopConfig == null) {
        this.hadoopConfig = new Configuration(); // get hadoop configuration
      } else {
        this.hadoopConfig = hadoopConfig;
      }
      String hadoopSSLConfigFile =
          this.hadoopConfig.get(resolveHadoopPropertyName(HADOOP_SSL_CONF_TPL_KEY, mode));
      logger.debug("Using Hadoop configuration for SSL");
      logger.debug("Hadoop SSL configuration file: {}", hadoopSSLConfigFile);
      this.hadoopConfig.addResource(hadoopSSLConfigFile);
    } else {
      this.hadoopConfig = null;
    }
  }

  protected String getConfigParam(String name, String hadoopName) {
    String value = "";
    if (hadoopConfig != null) {
      value = getHadoopConfigParam(hadoopName);
    }
    if (value.isEmpty() && config.hasPath(name)) {
      value = config.getString(name);
    }
    value = value.trim();
    return value;
  }

  protected String getHadoopConfigParam(String name) {
    Preconditions.checkArgument(this.hadoopConfig != null);
    String value = "";
    value = hadoopConfig.get(name, "");
    value = value.trim();
    return value;
  }

  protected String getConfigParamWithDefault(String name, String defaultValue) {
    String value = "";
    if (config.hasPath(name)) {
      value = config.getString(name);
    }
    if (value.isEmpty()) {
      value = defaultValue;
    }
    value = value.trim();
    return value;
  }

  protected String resolveHadoopPropertyName(String nameTemplate, SSLFactory.Mode mode) {
    return MessageFormat.format(nameTemplate, mode.toString().toLowerCase());
  }

  public abstract void validateKeyStore() throws DrillException;

  public abstract SslContext initSslContext() throws DrillException;

  public abstract boolean isUserSslEnabled();

  public abstract boolean isHttpsEnabled();

  public abstract String getKeyStoreType();

  public abstract String getKeyStorePath();

  public abstract String getKeyStorePassword();

  public abstract String getKeyPassword();

  public abstract String getTrustStoreType();

  public abstract boolean hasTrustStorePath();

  public abstract String getTrustStorePath();

  public abstract boolean hasTrustStorePassword();

  public abstract String getTrustStorePassword();

  public abstract String getProtocol();

  public abstract int getHandshakeTimeout();

  public abstract SSLFactory.Mode getMode();

  public abstract boolean isEnableHostVerification();

  public abstract boolean isDisableCertificateVerification();


  public abstract boolean isSslValid();

  public SslContext getSslContext() {
    return sslContext;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SSL is ")
        .append(isUserSslEnabled()?"":" not ")
        .append("enabled.\n");
    sb.append("HTTPS is ")
        .append(isHttpsEnabled()?"":" not ")
        .append("enabled.\n");
    if(isUserSslEnabled() || isHttpsEnabled()) {
      sb.append("SSL Configuration :")
          .append("\n\tprotocol: ").append(getProtocol())
          .append("\n\tkeyStoreType: ").append(getKeyStoreType())
          .append("\n\tkeyStorePath: ").append(getKeyStorePath())
          .append("\n\tkeyStorePassword: ").append(getPrintablePassword(getKeyStorePassword()))
          .append("\n\tkeyPassword: ").append(getPrintablePassword(getKeyPassword()))
          .append("\n\ttrustStoreType: ").append(getTrustStoreType())
          .append("\n\ttrustStorePath: ").append(getTrustStorePath())
          .append("\n\ttrustStorePassword: ").append(getPrintablePassword(getTrustStorePassword()))
          .append("\n\thandshakeTimeout: ").append(getHandshakeTimeout())
          .append("\n\tenableHostVerification: ").append(isEnableHostVerification())
          .append("\n\tdisableCertificateVerification: ").append(isDisableCertificateVerification())
      ;
    }
    return sb.toString();
  }

  private String getPrintablePassword(String password) {
    StringBuilder sb = new StringBuilder();
    if(password == null || password.length()<2 ){
      return password;
    }
    sb.append(password.charAt(0)).append("****").append(password.charAt(password.length()-1));
    return sb.toString();
  }
}
