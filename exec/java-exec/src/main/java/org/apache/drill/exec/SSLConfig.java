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
package org.apache.drill.exec;

import com.google.common.base.Preconditions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.DrillConfigurationException;
import org.apache.drill.common.exceptions.DrillException;
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

public class SSLConfig {


  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(
      SSLConfig.class);

  private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
  private static final int DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS = 10*1000; // 10 seconds

  private final boolean userSslEnabled;
  private final boolean httpsEnabled;

  private final String protocol;
  private final String keyStoreType;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String trustStoreType;
  private final String keyPassword;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final int handshakeTimeout;
  private final SSLContext sslContext;
  private final DrillConfig config;
  private final Configuration hadoopConfig;
  private final SSLFactory.Mode mode; // Let's reuse Hadoop's SSLFactory.Mode to distinguish client/server
  private final boolean disableHostVerification;
  private final boolean disableCertificateVerification;

  public static final String HADOOP_SSL_CONF_TPL_KEY =
      "hadoop.ssl.{0}.conf";
  public static final String HADOOP_SSL_KEYSTORE_LOCATION_TPL_KEY =
      "ssl.{0}.keystore.location";
  public static final String HADOOP_SSL_KEYSTORE_PASSWORD_TPL_KEY =
      "ssl.{0}.keystore.password";
  public static final String HADOOP_SSL_KEYSTORE_TYPE_TPL_KEY =
      "ssl.{0}.keystore.type";
  public static final String HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY =
      "ssl.{0}.truststore.location";
  public static final String HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY =
      "ssl.{0}.truststore.password";
  public static final String HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY =
      "ssl.{0}.truststore.type";

  private SSLConfig(DrillConfig config, Configuration hadoopConfig, SSLFactory.Mode mode, boolean initContext, boolean validateKeyStore)
      throws DrillException {

    this.config = config;
    this.mode = mode;
    if (this.mode == SSLFactory.Mode.SERVER) {
      userSslEnabled = config.hasPath(ExecConstants.USER_SSL_ENABLED) && config
          .getBoolean(ExecConstants.USER_SSL_ENABLED);
    } else {
      userSslEnabled =
          config.hasPath(DrillProperties.ENABLE_TLS) && config.getBoolean(DrillProperties.ENABLE_TLS);
    }
    httpsEnabled =
        config.hasPath(ExecConstants.HTTP_ENABLE_SSL) && config.getBoolean(ExecConstants.HTTP_ENABLE_SSL);

    // For testing we will mock up a hadoop configuration, however for regular use, we find the actual hadoop config.
    boolean enableHadoopConfig = config.getBoolean(ExecConstants.SSL_USE_HADOOP_CONF);
    if (enableHadoopConfig) {
      if(hadoopConfig == null) {
        this.hadoopConfig = new Configuration(); // get hadoop configuration
      } else {
        this.hadoopConfig = hadoopConfig;
      }
      String hadoopSSLConfigFile =
          this.hadoopConfig.get(resolveHadoopPropertyName(HADOOP_SSL_CONF_TPL_KEY));
      this.hadoopConfig.addResource(hadoopSSLConfigFile);
    } else {
      this.hadoopConfig = null;
    }

    keyStoreType = getConfigParam(ExecConstants.SSL_KEYSTORE_TYPE,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_TYPE_TPL_KEY));
    keyStorePath = getConfigParam(ExecConstants.SSL_KEYSTORE_PATH,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_LOCATION_TPL_KEY));
    keyStorePassword = getConfigParam(ExecConstants.SSL_KEYSTORE_PASSWORD,
        resolveHadoopPropertyName(HADOOP_SSL_KEYSTORE_PASSWORD_TPL_KEY));
    // if no keypassword specified, use keystore password
    keyPassword = getConfigParamWithDefault(ExecConstants.SSL_KEY_PASSWORD, keyStorePassword);
    if (this.mode == SSLFactory.Mode.SERVER) {
      trustStoreType = getConfigParam(ExecConstants.SSL_TRUSTSTORE_TYPE,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY));
      trustStorePath = getConfigParam(ExecConstants.SSL_TRUSTSTORE_PATH,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY));
      trustStorePassword = getConfigParam(ExecConstants.SSL_TRUSTSTORE_PASSWORD,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
      disableHostVerification = false;
      disableCertificateVerification = false;
    } else {
      trustStoreType = getConfigParam(DrillProperties.TRUSTSTORE_TYPE,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_TYPE_TPL_KEY));
      trustStorePath = getConfigParam(DrillProperties.TRUSTSTORE_PATH,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_LOCATION_TPL_KEY));
      trustStorePassword = getConfigParam(DrillProperties.TRUSTSTORE_PASSWORD,
          resolveHadoopPropertyName(HADOOP_SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
      disableHostVerification = config.hasPath(DrillProperties.DISABLE_HOST_VERIFICATION) && config
          .getBoolean(DrillProperties.DISABLE_HOST_VERIFICATION);
      disableCertificateVerification = config.hasPath(DrillProperties.DISABLE_CERT_VERIFICATION) && config
          .getBoolean(DrillProperties.DISABLE_CERT_VERIFICATION);
    }
    protocol = getConfigParamWithDefault(ExecConstants.SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
    int hsTimeout = config.hasPath(ExecConstants.SSL_HANDSHAKE_TIMEOUT) ?
        config.getInt(ExecConstants.SSL_HANDSHAKE_TIMEOUT) :
        DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
    if (hsTimeout <= 0) {
      hsTimeout = DEFAULT_SSL_HANDSHAKE_TIMEOUT_MS;
    }
    handshakeTimeout = hsTimeout;

    if (initContext) {
      sslContext = initSSLContext(mode);
    } else {
      sslContext = null;
    }
    //HTTPS validates the keystore is not empty. User Server SSL context initialization also validates keystore, but
    // much more strictly. User Client context initialization does not validate keystore.
    /*If keystorePath or keystorePassword is provided in the configuration file use that*/
    if ((isUserSslEnabled() || isHttpsEnabled() ) && validateKeyStore) {
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

  private String getConfigParam(String name, String hadoopName) {
    String value = "";
    if (config.hasPath(name)) {
      value = config.getString(name);
    }
    if (value.isEmpty() && hadoopConfig != null) {
      value = getHadoopConfigParam(hadoopName);
    }
    value = value.trim();
    return value;
  }

  private String getHadoopConfigParam(String name) {
    Preconditions.checkArgument(this.hadoopConfig != null);
    String value = "";
    value = hadoopConfig.get(name, "");
    value = value.trim();
    return value;
  }

  private String getConfigParamWithDefault(String name, String defaultValue) {
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

  private String resolveHadoopPropertyName(String nameTemplate){
    return MessageFormat.format(nameTemplate, mode.toString().toLowerCase());
  }

  private SSLContext initSSLContext(SSLFactory.Mode mode) throws DrillException {
    final SSLContext sslCtx;

    if (!userSslEnabled) {
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
      if (mode == SSLFactory.Mode.SERVER && keyStorePath.isEmpty()) {
        throw new DrillException("No Keystore provided.");
      }
      if (!keyStorePath.isEmpty()) {
        KeyStore ks =
            KeyStore.getInstance(!keyStoreType.isEmpty() ? keyStoreType : KeyStore.getDefaultType());
        try {
          // Will throw an exception if the file is not found/accessible.
          InputStream ksStream = new FileInputStream(keyStorePath);
          // A key password CANNOT be null or an empty string.
          if (mode == SSLFactory.Mode.SERVER && keyStorePassword.isEmpty()) {
            throw new DrillException("The Keystore password cannot be empty.");
          }
          ks.load(ksStream, keyStorePassword.toCharArray());
          // Empty Keystore. (Remarkably, it is possible to do this).
          if (ks.size() == 0 && mode == SSLFactory.Mode.SERVER) {
            throw new DrillException("The Keystore has no entries.");
          }
          KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(ks, keyPassword.toCharArray());
          kms = kmf.getKeyManagers(); // Will throw an exception if the key password is not correct
        } catch (Exception e) {
          if (mode == SSLFactory.Mode.SERVER) {
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
      String truststoretype = trustStoreType;
      String truststorepath = trustStorePath;
      String truststorepassword = trustStorePassword;
      if (trustStorePath.isEmpty()) {
        truststoretype = keyStoreType;
        truststorepath = keyStorePath;
        truststorepassword = keyStorePassword;
      }
      if (!truststorepath.isEmpty()) {
        // use default keystore type. user can customize by specifying javax.net.ssl.trustStoreType
        KeyStore ts =
            KeyStore.getInstance(!truststoretype.isEmpty() ? truststoretype : KeyStore.getDefaultType());
        InputStream tsStream = new FileInputStream(truststorepath);
        ts.load(tsStream, truststorepassword.toCharArray());
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        tms = tmf.getTrustManagers();
      }

      sslCtx = SSLContext.getInstance(protocol);
      sslCtx.init(kms, tms, null);
    } catch (Exception e) {
      // Catch any SSL initialization Exceptions here and abort.
      throw new DrillException(new StringBuilder()
          .append("SSL is enabled but cannot be initialized due to the following exception: ")
          .append(e.getMessage()).toString());
    }
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

  public boolean hasTrustStorePassword() {  return !trustStorePassword.isEmpty(); }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getProtocol() {
    return protocol;
  }

  public int getHandshakeTimeout() {
    return handshakeTimeout;
  }

  public boolean isSslValid() {  return !keyStorePath.isEmpty() && !keyStorePassword.isEmpty(); }

  public SSLContext getSslContext() {
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
          .append("\n\tprotocol: ").append(protocol)
          .append("\n\tkeyStoreType: ") .append(keyStoreType)
          .append("\n\tkeyStorePath: ") .append(keyStorePath)
          .append("\n\tkeyStorePassword: ") .append(keyStorePassword)
          .append("\n\tkeyPassword: ").append(keyPassword)
          .append("\n\ttrustStoreType: ") .append(trustStoreType)
          .append("\n\ttrustStorePath: ") .append(trustStorePath)
          .append("\n\ttrustStorePassword: ") .append(trustStorePassword)
          .append("\n\thandshakeTimeout: ").append(handshakeTimeout);
    }

    return sb.toString();
  }

  public static class SSLConfigBuilder {
    private DrillConfig config = null;
    private Configuration hadoopConfig = null;
    private SSLFactory.Mode mode = SSLFactory.Mode.SERVER;
    private boolean initializeSSLContext = false;
    private boolean validateKeyStore = false;

    public SSLConfigBuilder(){

    }

    public SSLConfig build() throws DrillException{
      if (config == null){
        throw new DrillConfigurationException("Cannot create SSL configuration from null Drill configuration.");
      }
      SSLConfig sslConfig;
      sslConfig = new SSLConfig(config, hadoopConfig, mode, initializeSSLContext, validateKeyStore);
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
}
