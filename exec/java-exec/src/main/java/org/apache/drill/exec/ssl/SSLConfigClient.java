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

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SSLConfigClient extends SSLConfig {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfigClient.class);

  private final SSLFactory.Mode mode; // Let's reuse Hadoop's SSLFactory.Mode to distinguish client/server
  private final boolean userSslEnabled;
  private final String trustStoreType;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final boolean enableHostVerification;
  private final boolean disableCertificateVerification;
  private final String protocol;
  private final int handshakeTimeout;
  private final String provider;

  private final String emptyString = new String();

  public SSLConfigClient(DrillConfig config, Configuration hadoopConfig, boolean initContext,
      boolean validateKeyStore) throws DrillException {
    super(config, hadoopConfig, SSLFactory.Mode.CLIENT);
    this.mode = SSLFactory.Mode.CLIENT;
    userSslEnabled =
        config.hasPath(DrillProperties.ENABLE_TLS) && config.getBoolean(DrillProperties.ENABLE_TLS);
    trustStoreType = getConfigParam(DrillProperties.TRUSTSTORE_TYPE,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY, mode));
    trustStorePath = getConfigParam(DrillProperties.TRUSTSTORE_PATH,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY, mode));
    trustStorePassword = getConfigParam(DrillProperties.TRUSTSTORE_PASSWORD,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY, mode));
    enableHostVerification = config.hasPath(DrillProperties.ENABLE_HOST_VERIFICATION) && config
        .getBoolean(DrillProperties.ENABLE_HOST_VERIFICATION);
    disableCertificateVerification = config.hasPath(DrillProperties.DISABLE_CERT_VERIFICATION) && config
        .getBoolean(DrillProperties.DISABLE_CERT_VERIFICATION);
    protocol = getConfigParamWithDefault(DrillProperties.TLS_PROTOCOL, DEFAULT_SSL_PROTOCOL);
    int hsTimeout = config.hasPath(DrillProperties.TLS_HANDSHAKE_TIMEOUT) ?
        config.getInt(DrillProperties.TLS_HANDSHAKE_TIMEOUT) :
        DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
    if (hsTimeout <= 0) {
      hsTimeout = DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
    }
    handshakeTimeout = hsTimeout;
    provider = getConfigParamWithDefault(DrillProperties.TLS_PROVIDER, DEFAULT_SSL_PROVIDER);
  }

  public void validateKeyStore() throws DrillException {

  }

  public SslContext initSslContext() throws DrillException {
    final SslContext sslCtx;

    if (!userSslEnabled) {
      return null;
    }

    try {
      KeyStore ts = null;
      // if truststore is not provided then we will use the default. Note that the default depends on
      // the TrustManagerFactory that in turn depends on the Security Provider
      if (!trustStorePath.isEmpty()) {
        ts = KeyStore.getInstance(!trustStoreType.isEmpty() ? trustStoreType : KeyStore.getDefaultType());
        InputStream tsStream = new FileInputStream(trustStorePath);
        ts.load(tsStream, trustStorePassword.toCharArray());
      }
      TrustManagerFactory tmf;
      if (disableCertificateVerification) {
        tmf = InsecureTrustManagerFactory.INSTANCE;
      } else {
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      }
      tmf.init(ts);
      sslCtx = SslContextBuilder.forClient()
          .sslProvider(getProvider())
          .trustManager(tmf)
          .protocols(protocol)
          .build();
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(new StringBuilder()
          .append("SSL is enabled but cannot be initialized due to the following exception: ")
          .append(e.getMessage()).toString());
    }
    this.sslContext = sslCtx;
    return sslCtx;
  }

  public boolean isUserSslEnabled() {
    return userSslEnabled;
  }

  public boolean isHttpsEnabled() {
    return httpsEnabled;
  }

  public String getKeyStoreType() {
    return emptyString;
  }

  public String getKeyStorePath() {
    return emptyString;
  }

  public String getKeyStorePassword() {
    return emptyString;
  }

  public String getKeyPassword() {
    return emptyString;
  }

  public String getTrustStoreType() {
    return trustStoreType;
  }

  public boolean hasTrustStorePath() {
    return !trustStorePath.isEmpty();
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public boolean hasTrustStorePassword() {
    return !trustStorePassword.isEmpty();
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getProtocol() {
    return protocol;
  }

  public SslProvider getProvider() {
    return provider.equalsIgnoreCase("JDK") ? SslProvider.JDK : SslProvider.OPENSSL;
  }

  public int getHandshakeTimeout() {
    return handshakeTimeout;
  }

  public SSLFactory.Mode getMode() {
    return mode;
  }

  public boolean isEnableHostVerification() {
    return enableHostVerification;
  }

  public boolean isDisableCertificateVerification() {
    return disableCertificateVerification;
  }

  public boolean isSslValid() {
    return true;
  }

  public SslContext getSslContext() {
    return sslContext;
  }

}
