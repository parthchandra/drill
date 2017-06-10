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
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SSLConfigServer extends SSLConfig {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfigServer.class);

  private final SSLFactory.Mode mode; // Let's reuse Hadoop's SSLFactory.Mode to distinguish client/server
  private final boolean userSslEnabled;
  private final String keyStoreType;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String keyPassword;
  private final String trustStoreType;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final String protocol;
  private final String provider;

  public SSLConfigServer(DrillConfig config, Configuration hadoopConfig) throws DrillException {
    super(config, hadoopConfig, SSLFactory.Mode.SERVER);
    this.mode = SSLFactory.Mode.SERVER;
    userSslEnabled =
        config.hasPath(ExecConstants.USER_SSL_ENABLED) && config.getBoolean(ExecConstants.USER_SSL_ENABLED);
    trustStoreType = getConfigParam(ExecConstants.SSL_TRUSTSTORE_TYPE,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY, mode));
    trustStorePath = getConfigParam(ExecConstants.SSL_TRUSTSTORE_PATH,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY, mode));
    trustStorePassword = getConfigParam(ExecConstants.SSL_TRUSTSTORE_PASSWORD,
        resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY, mode));
    keyStoreType = getConfigParam(ExecConstants.SSL_KEYSTORE_TYPE,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_TYPE_TPL_KEY, mode));
    keyStorePath = getConfigParam(ExecConstants.SSL_KEYSTORE_PATH,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_LOCATION_TPL_KEY, mode));
    keyStorePassword = getConfigParam(ExecConstants.SSL_KEYSTORE_PASSWORD,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_PASSWORD_TPL_KEY, mode));
    // if no keypassword specified, use keystore password
    String keyPass = getConfigParam(ExecConstants.SSL_KEY_PASSWORD,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_KEYPASSWORD_TPL_KEY, mode));
    keyPassword = keyPass.isEmpty() ? keyStorePassword : keyPass;
    protocol = getConfigParamWithDefault(ExecConstants.SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
    provider = getConfigParamWithDefault(ExecConstants.SSL_PROVIDER, DEFAULT_SSL_PROVIDER);
  }

  public void validateKeyStore() throws DrillException {
    //HTTPS validates the keystore is not empty. User Server SSL context initialization also validates keystore, but
    // much more strictly. User Client context initialization does not validate keystore.
    /*If keystorePath or keystorePassword is provided in the configuration file use that*/
    if ((isUserSslEnabled() || isHttpsEnabled())) {
      if (!keyStorePath.isEmpty() || !keyStorePassword.isEmpty()) {
        if (keyStorePath.isEmpty()) {
          throw new DrillException(
              " *.ssl.keyStorePath in the configuration file is empty, but *.ssl.keyStorePassword is set");
        } else if (keyStorePassword.isEmpty()) {
          throw new DrillException(
              " *.ssl.keyStorePassword in the configuration file is empty, but *.ssl.keyStorePath is set ");
        }
      }
    }
  }

  @Override
  public SslContext initSslContext() throws DrillException {
    final SslContext sslCtx;

    if (!userSslEnabled) {
      return null;
    }

    KeyManagerFactory kmf;
    TrustManagerFactory tmf;
    try {
      if (keyStorePath.isEmpty()) {
        throw new DrillException("No Keystore provided.");
      }
      kmf = initializeKeyManagerFactory();
      tmf = initializeTrustManagerFactory();
      sslCtx = SslContextBuilder.forServer(kmf)
          .trustManager(tmf)
          .protocols(protocol)
          .build(); // Will throw an exception if the key password is not correct
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(new StringBuilder()
          .append("SSL is enabled but cannot be initialized - ")
          .append("[ ")
          .append(e.getMessage())
          .append("]. ")
          .toString());
    }
    this.nettySslContext = sslCtx;
    return sslCtx;
  }

  @Override
  public SSLContext initSSLContext() throws DrillException {
    final SSLContext sslCtx;

    if (!userSslEnabled) {
      return null;
    }

    KeyManagerFactory kmf;
    TrustManagerFactory tmf;
    try {
      if (keyStorePath.isEmpty()) {
        throw new DrillException("No Keystore provided.");
      }
      kmf = initializeKeyManagerFactory();
      tmf = initializeTrustManagerFactory();
      sslCtx = SSLContext.getInstance(protocol);
      sslCtx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(
          new StringBuilder().append("SSL is enabled but cannot be initialized - ")
              .append("[ ")
              .append(e.getMessage())
              .append("]. ")
              .toString());
    }
    this.jdkSSlContext = sslCtx;
    return sslCtx;
  }


  @Override
  public boolean isUserSslEnabled() {
    return userSslEnabled;
  }

  @Override
  public boolean isHttpsEnabled() {
    return httpsEnabled;
  }

  @Override
  public String getKeyStoreType() {
    return keyStoreType;
  }

  @Override
  public String getKeyStorePath() {
    return keyStorePath;
  }

  @Override
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  @Override
  public String getKeyPassword() {
    return keyPassword;
  }

  @Override
  public String getTrustStoreType() {
    return trustStoreType;
  }

  @Override
  public boolean hasTrustStorePath() {
    return !trustStorePath.isEmpty();
  }

  @Override
  public String getTrustStorePath() {
    return trustStorePath;
  }

  @Override
  public boolean hasTrustStorePassword() {
    return !trustStorePassword.isEmpty();
  }

  @Override
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  @Override
  public String getProtocol() {
    return protocol;
  }

  @Override
  public SslProvider getProvider() {
    return provider.equalsIgnoreCase("JDK") ? SslProvider.JDK : SslProvider.OPENSSL;
  }

  @Override
  public int getHandshakeTimeout() {
    return 0;
  }

  @Override
  public SSLFactory.Mode getMode() {
    return mode;
  }

  @Override
  public boolean disableHostVerification() {
    return false;
  }

  @Override
  public boolean disableCertificateVerification() {
    return false;
  }

  @Override
  public boolean useSystemTrustStore() {
    return false; // Client only, notsupported by the server
  }

  public boolean isSslValid() {
    return !keyStorePath.isEmpty() && !keyStorePassword.isEmpty();
  }

}
