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
package org.apache.drill.exec.rpc.user;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.BootStrapContext;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;

// SSL config for bit to user connection
// package private
public class SSLConfig {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfig.class);

  private static final String DEFAULT_SSL_PROTOCOL = new String("TLSv1.2");

  private final boolean sslEnabled;

  //private final String keystoreType;
  private final String keystorePath;
  private final String keystorePassword;
  private final String truststorePath;
  private final String truststorePassword;
  private final String protocol;
  private final SSLContext sslContext;

  SSLConfig(BootStrapContext context) throws DrillException {
    this(context.getConfig());
  }

  SSLConfig(DrillConfig config) throws DrillException {
    if (sslEnabled = config.hasPath(ExecConstants.USER_SSL_ENABLED) && config
        .getBoolean(ExecConstants.USER_SSL_ENABLED)) {
      //keystoreType = config.getString(ExecConstants.USER_SSL_KEYSTORE_TYPE);
      keystorePath = config.hasPath(ExecConstants.USER_SSL_KEYSTORE_PATH) ?
          config.getString(ExecConstants.USER_SSL_KEYSTORE_PATH) :
          null;
      keystorePassword = config.hasPath(ExecConstants.USER_SSL_KEYSTORE_PASSWORD) ?
          config.getString(ExecConstants.USER_SSL_KEYSTORE_PASSWORD) :
          null;
      truststorePath = config.hasPath(ExecConstants.USER_SSL_TRUSTSTORE_PATH) ?
          config.getString(ExecConstants.USER_SSL_TRUSTSTORE_PATH) :
          null;
      truststorePassword = config.hasPath(ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD) ?
          config.getString(ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD) :
          null;
      protocol = config.hasPath(ExecConstants.USER_SSL_PROTOCOL) ?
          config.getString(ExecConstants.USER_SSL_PROTOCOL) :
          DEFAULT_SSL_PROTOCOL;

      //TODO: Validate SSL input. if null, get from System/Security properties ?

      sslContext = init();

    } else {
      //keystoreType = null;
      keystorePath = null;
      keystorePassword = null;
      truststorePath = null;
      truststorePassword = null;
      protocol = null;
      sslContext = null;
    }
  }

  SSLConfig(DrillProperties properties) throws DrillException {
    if( sslEnabled = properties.containsKey(ExecConstants.USER_SSL_ENABLED) && Boolean
        .parseBoolean(properties.getProperty(ExecConstants.USER_SSL_ENABLED))) {
      //keystoreType = config.getString(ExecConstants.USER_SSL_KEYSTORE_TYPE);
      keystorePath = properties.containsKey(ExecConstants.USER_SSL_KEYSTORE_PATH) ?
          properties.getProperty(ExecConstants.USER_SSL_KEYSTORE_PATH) :
          null;
      keystorePassword = properties.containsKey(ExecConstants.USER_SSL_KEYSTORE_PASSWORD) ?
          properties.getProperty(ExecConstants.USER_SSL_KEYSTORE_PASSWORD) :
          null;
      truststorePath = properties.containsKey(ExecConstants.USER_SSL_TRUSTSTORE_PATH) ?
          properties.getProperty(ExecConstants.USER_SSL_TRUSTSTORE_PATH) :
          null;
      truststorePassword = properties.containsKey(ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD) ?
          properties.getProperty(ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD) :
          null;
      protocol = properties.containsKey(ExecConstants.USER_SSL_PROTOCOL) ?
          properties.getProperty(ExecConstants.USER_SSL_PROTOCOL) :
          DEFAULT_SSL_PROTOCOL;

      //TODO: Validate input. if null, get from System properties or Security properties

      sslContext = init();

    } else {
      //keystoreType = null;
      keystorePath = null;
      keystorePassword = null;
      truststorePath = null;
      truststorePassword = null;
      protocol = null;
      sslContext = null;
    }

  }

  private SSLContext init() throws  DrillException {
    final String keypassword = getKeystorePassword();
    final String keystore = getKeystorePath();
    final String trustpassword = getTruststorePassword();
    final String truststore = getTruststorePath();
    final String sslProtocol = getProtocol();
    final SSLContext sslCtx;
    try {

      final TrustManager[] tms;
      final KeyManager[] kms;

      if( keystore != null) {
        // use default. user can customize by specifying javax.net.ssl.keyStoreType
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(ClassLoader.class.getResourceAsStream(keystore), keypassword.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keypassword.toCharArray());
        kms = kmf.getKeyManagers();
      } else {
        kms = null;
      }

      if(truststore != null) {
        // use default. user can customize by specifying javax.net.ssl.trustStoreType
        KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
        ts.load(ClassLoader.class.getResourceAsStream(truststore), trustpassword.toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        tms = tmf.getTrustManagers();

      } else {
        tms = null;
      }

      sslCtx = SSLContext.getInstance(sslProtocol);
      sslCtx.init(kms, tms, null);
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(
          "SSL is enabled but cannot be initialized due to the following exception.", e);
    }
    return sslCtx;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  //public String getKeystoreType() {
  //  return keystoreType;
  //}

  public String getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public String getProtocol() {
    return protocol;
  }

  public SSLContext getSslContext() {
    return sslContext;
  }
}

