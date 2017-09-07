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
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillConfigurationException;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.text.MessageFormat;

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
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_PASSWORD_TPL_KEY, mode));
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
      KeyStore ks =
          KeyStore.getInstance(!keyStoreType.isEmpty() ? keyStoreType : KeyStore.getDefaultType());
      try {
        //initialize the key manager factory
        // Will throw an exception if the file is not found/accessible.
        InputStream ksStream = new FileInputStream(keyStorePath);
        // A key password CANNOT be null or an empty string.
        if (keyStorePassword.isEmpty()) {
          throw new DrillException("The Keystore password cannot be empty.");
        }
        ks.load(ksStream, keyStorePassword.toCharArray());
        // Empty Keystore. (Remarkably, it is possible to do this).
        if (ks.size() == 0) {
          throw new DrillException("The Keystore has no entries.");
        }
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyPassword.toCharArray());

      } catch (Exception e) {
        throw new DrillException(new StringBuilder()
            .append("Exception while initializing the keystore: ")
            .append(e.getMessage()).toString());
      }
      try {
        //initialize the trust manager factory
        KeyStore ts = null;
        // if truststore is not provided then we will use the default. Note that the default depends on
        // the TrustManagerFactory that in turn depends on the Security Provider
        if (!trustStorePath.isEmpty()) {
          ts = KeyStore.getInstance(!trustStoreType.isEmpty() ? trustStoreType : KeyStore.getDefaultType());
          InputStream tsStream = new FileInputStream(trustStorePath);
          ts.load(tsStream, trustStorePassword.toCharArray());
        }
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
      } catch (Exception e) {
        throw new DrillException(new StringBuilder()
            .append("Exception while initializing the truststore: ")
            .append(e.getMessage()).toString());
      }
      sslCtx = SslContextBuilder.forServer(kmf)
          .trustManager(tmf)
          .protocols(protocol)
          .sslProvider(getProvider())
          .build(); // Will throw an exception if the key password is not correct
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(new StringBuilder()
          .append("SSL is enabled but cannot be initialized - ")
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
    return keyStoreType;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getKeyPassword() {
    return keyPassword;
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
    return 0;
  }

  public SSLFactory.Mode getMode() {
    return mode;
  }

  public boolean isEnableHostVerification() {
    return false;
  }

  public boolean isDisableCertificateVerification() {
    return false;
  }

  public boolean isSslValid() {
    return !keyStorePath.isEmpty() && !keyStorePassword.isEmpty();
  }

  public SslContext getSslContext() {
    return sslContext;
  }

}
