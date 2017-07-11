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
package org.apache.drill.exec.rpc;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.BootStrapContext;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

// SSL config for bit to user connection
// package private
public class SSLConfig {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfig.class);

  private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
  private static final int DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS = 10*1000; // 10 seconds

  private final boolean sslEnabled;

  private final String keystoreType;
  private final String keystorePath;
  private final String keystorePassword;
  private final String keyPassword;
  private final String truststoreType;
  private final String truststorePath;
  private final String truststorePassword;
  private final String protocol;
  private final int handshakeTimeout;
  private final SSLContext sslContext;

  public SSLConfig(DrillConfig config, boolean validateKeyStore) throws DrillException {
    if (sslEnabled = config.hasPath(ExecConstants.USER_SSL_ENABLED) && config
        .getBoolean(ExecConstants.USER_SSL_ENABLED)) {
      keystoreType = getSystemConfigParam(config, ExecConstants.USER_SSL_KEYSTORE_TYPE);
      keystorePath = getSystemConfigParam(config, ExecConstants.USER_SSL_KEYSTORE_PATH);
      keystorePassword = getSystemConfigParam(config, ExecConstants.USER_SSL_KEYSTORE_PASSWORD);
      // if no keypassword specified, use keystore password
      keyPassword = getConfigParam(config, ExecConstants.USER_SSL_KEY_PASSWORD, keystorePassword);
      truststoreType = getSystemConfigParam(config, ExecConstants.USER_SSL_TRUSTSTORE_TYPE);
      truststorePath = getSystemConfigParam(config, ExecConstants.USER_SSL_TRUSTSTORE_PATH);
      truststorePassword = getSystemConfigParam(config, ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD);
      protocol = getConfigParam(config, ExecConstants.USER_SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
      int hsTimeout = config.hasPath(ExecConstants.USER_SSL_HANDSHAKE_TIMEOUT) ?
          config.getInt(ExecConstants.USER_SSL_HANDSHAKE_TIMEOUT) :
          DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
      if (hsTimeout <= 0) {
        hsTimeout = DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
      }
      handshakeTimeout = hsTimeout;
      sslContext = init(validateKeyStore);
    } else {
      keystoreType = null;
      keystorePath = null;
      keystorePassword = null;
      keyPassword = null;
      truststoreType = null;
      truststorePath = null;
      truststorePassword = null;
      protocol = null;
      handshakeTimeout = 0;
      sslContext = null;
    }
  }

  public SSLConfig(DrillProperties properties, boolean validateKeyStore) throws DrillException {
    if( sslEnabled = properties.containsKey(ExecConstants.USER_SSL_ENABLED) && Boolean
        .parseBoolean(properties.getProperty(ExecConstants.USER_SSL_ENABLED))) {
      keystoreType = getSystemProp(properties, ExecConstants.USER_SSL_KEYSTORE_TYPE);
      keystorePath = getSystemProp(properties, ExecConstants.USER_SSL_KEYSTORE_PATH);
      keystorePassword = getSystemProp(properties, ExecConstants.USER_SSL_KEYSTORE_PASSWORD);
      // if no keypassword specified, use keystore password
      keyPassword = getProp(properties, ExecConstants.USER_SSL_KEY_PASSWORD, keystorePassword);
      truststoreType = getSystemProp(properties, ExecConstants.USER_SSL_TRUSTSTORE_TYPE);
      truststorePath = getSystemProp(properties, ExecConstants.USER_SSL_TRUSTSTORE_PATH);
      truststorePassword = getSystemProp(properties, ExecConstants.USER_SSL_TRUSTSTORE_PASSWORD);
      protocol = getProp(properties, ExecConstants.USER_SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
      int hsTimeout = properties.containsKey(ExecConstants.USER_SSL_HANDSHAKE_TIMEOUT) ?
          Integer.parseInt(properties.getProperty(ExecConstants.USER_SSL_HANDSHAKE_TIMEOUT)) :
          DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
      if (hsTimeout <= 0) {
        hsTimeout = DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
      }
      handshakeTimeout = hsTimeout;
      sslContext = init(validateKeyStore);
    } else {
      keystoreType = null;
      keystorePath = null;
      keystorePassword = null;
      keyPassword = null;
      truststoreType = null;
      truststorePath = null;
      truststorePassword = null;
      protocol = null;
      handshakeTimeout = 0;
      sslContext = null;
    }
  }

  private SSLContext init(boolean validateKeystore) throws  DrillException {
    final String keystoretype = getKeystoreType();
    final String keystorepassword = getKeystorePassword();
    final String keystore = getKeystorePath();
    final String keypassword = getKeyPassword();
    String truststoretype = getTruststoreType();
    String truststore = getTruststorePath();
    String truststorepassword = getTruststorePassword();
    final String sslProtocol = getProtocol();
    final SSLContext sslCtx;

    if(!sslEnabled){
      return null;
    }

    try {

      TrustManager[] tms = null;
      KeyManager[] kms = null;

      /**
       * If a keystore path is provided, and the keystoreValidate flag is true (as in the server),
       * we validate all other input and make sure the KeyManagers array is initialized with at least
       * one not null value. Otherwise the SSLContext will get initialized with an empty list and will
       * not be able to allow any client to connect.
       * If the ssl config is being created by a client, then the KeyStore is not required and the keyStore
       * valdation flag should be false.
       */
      if(validateKeystore && keystore == null){
        throw new DrillException("No Keystore provided.");
      }
      if (keystore != null) {
        KeyStore ks = KeyStore.getInstance(keystoretype != null ? keystoretype : KeyStore.getDefaultType());
        try {
          // Will throw an exception if the file is not found/accessible.
          InputStream ksStream = new FileInputStream(keystore);
          // A key password CANNOT be null or an empty string.
          if( validateKeystore && (keystorepassword == null || keystorepassword.length() == 0)){
            throw new DrillException("The Keystore password is empty and is not allowed.");
          }
          ks.load(ksStream, keystorepassword == null ? null : keystorepassword.toCharArray());
          // Empty Keystore. (Remarkably, it is possible to do this).
          if (ks.size() == 0 && validateKeystore) {
            throw new DrillException("The Keystore has no entries.");
          }
          KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(ks, keypassword.toCharArray());
          kms = kmf.getKeyManagers(); // Will throw an exception if the key password is not correct
        } catch (Exception e) {
          if (validateKeystore) {
            throw e;
          }
        }
      }

      /**
       * If the user has not provided a trust store path and somehow the System properties do not have
       * defaults set, then use the values provided for the keystore. This can only happen if the user
       * has overridden the System properties to bogus values, but we cannot really guard ourselves
       * from that.
       * Note that it is perfectly legal, but not useful, to have a null trust manager list passed to
       * SSLContext.init
       */
      if(truststore == null){
        truststoretype = keystoretype;
        truststore= keystore;
        truststorepassword = keystorepassword;
      }
      if(truststore != null) {
        // use default keystore type. user can customize by specifying javax.net.ssl.trustStoreType
        KeyStore ts =
            KeyStore.getInstance(truststoretype != null ? truststoretype : KeyStore.getDefaultType());
        InputStream tsStream = new FileInputStream(truststore);
        ts.load(tsStream, truststorepassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        tms = tmf.getTrustManagers();
      }

      sslCtx = SSLContext.getInstance(sslProtocol);
      sslCtx.init(kms, tms, null);
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(new StringBuilder()
          .append("SSL is enabled but cannot be initialized due to the following exception: ")
          .append(e.getMessage()).toString());
    }
    return sslCtx;
  }

  private String getSystemConfigParam(DrillConfig config, String name) {
    String value;
    if (config.hasPath(name)) {
      value = config.getString(name);
    } else {
      if (System.getProperty(name) != null) {
        value = System.getProperty(name);
      } else {
        value = null;
      }
    }
    return value;
  }

  private String getConfigParam(DrillConfig config, String name, String defaultValue) {
    String value;
    if (config.hasPath(name)) {
      value = config.getString(name);
    } else {
      value = defaultValue;
    }
    return value;
  }

  private String getSystemProp(DrillProperties prop, String name) {
    String value;
    if (prop.containsKey(name)) {
      value = prop.getProperty(name);
    } else {
      if (System.getProperty(name) != null) {
        value = System.getProperty(name);
      } else {
        value = null;
      }
    }
    return value;
  }

  private String getProp(DrillProperties prop, String name, String defaultValue) {
    String value;
    if (prop.containsKey(name)) {
      value = prop.getProperty(name);
    } else {
      value = defaultValue;
    }
    return value;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public String getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public String getKeyPassword() {
    return keyPassword;
  }

  public String getTruststoreType() {
    return truststoreType;
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

  public int getHandshakeTimeout() {
    return handshakeTimeout;
  }

  public SSLContext getSslContext() {
    return sslContext;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if(sslEnabled) {
      sb.append("SSL Configuration :")
          .append( "keystoreType: ").append( keystoreType)
          .append( "keystorePath: ").append( keystorePath)
          .append( "keystorePassword: ").append( keystorePassword)
          .append( "keyPassword: ").append( keyPassword)
          .append( "truststoreType: ").append( truststoreType)
          .append( "truststorePath: ").append( truststorePath)
          .append( "truststorePassword: ").append( truststorePassword)
          .append( "protocol: ").append( protocol)
          .append( "handshakeTimeout: ").append( handshakeTimeout)
          ;

    } else {
      sb.append("SSL is not enabled.");
    }
    return sb.toString();
  }
}

