/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user.security;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import junit.framework.TestCase;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.NonTransientRpcException;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

@Ignore("See DRILL-5387")
public class TestUserBitSSL extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestUserBitSSL.class);

  private static DrillConfig newConfig;
  private static Properties initProps; // initial client properties
  private static ClassLoader classLoader;
  private static String ksPath;
  private static String tsPath;
  private static String emptyTSPath;

  @BeforeClass
  public static void setupTest() throws Exception {

    // Create a new DrillConfig
    classLoader = TestUserBitSSL.class.getClassLoader();
    ksPath = new File(classLoader.getResource("ssl/keystore.ks").getFile()).getAbsolutePath();
    tsPath = new File(classLoader.getResource("ssl/truststore.ks").getFile()).getAbsolutePath();
    emptyTSPath = new File(classLoader.getResource("ssl/emptytruststore.ks").getFile()).getAbsolutePath();
    newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_SSL_ENABLED,
            ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.SSL_KEYSTORE_TYPE,
            ConfigValueFactory.fromAnyRef("JKS"))
        .withValue(ExecConstants.SSL_KEYSTORE_PATH,
            ConfigValueFactory.fromAnyRef(ksPath))
        .withValue(ExecConstants.SSL_KEYSTORE_PASSWORD,
            ConfigValueFactory.fromAnyRef("drill123"))
        .withValue(ExecConstants.SSL_KEY_PASSWORD,
            ConfigValueFactory.fromAnyRef("drill123"))
        .withValue(ExecConstants.SSL_TRUSTSTORE_TYPE,
            ConfigValueFactory.fromAnyRef("JKS"))
        .withValue(ExecConstants.SSL_TRUSTSTORE_PATH,
            ConfigValueFactory.fromAnyRef(tsPath))
        .withValue(ExecConstants.SSL_TRUSTSTORE_PASSWORD,
            ConfigValueFactory.fromAnyRef("drill123"))
        .withValue(ExecConstants.SSL_PROTOCOL,
            ConfigValueFactory.fromAnyRef("TLSv1.2")),
      false);

    initProps = new Properties();
    initProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    initProps.setProperty(DrillProperties.TRUSTSTORE_PATH, tsPath);
    initProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");

    // Start an SSL enabled cluster
    updateTestCluster(1, newConfig, initProps);
  }

  @AfterClass
  public static void cleanTest() throws Exception {
  }

  @Test
  public void testSSLConnection() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, tsPath);
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      TestCase.fail( new StringBuilder()
          .append("SSL Connection failed with exception [" )
          .append( e.getMessage() )
          .append("]")
          .toString());
    }
  }

  @Test
  public void testSSLConnectionWithKeystore() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, ksPath);
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      TestCase.fail( new StringBuilder()
          .append("SSL Connection failed with exception [" )
          .append( e.getMessage() )
          .append("]")
          .toString());
    }
  }

  @Test
  public void testSSLConnectionFailBadTrustStore() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, ""); // NO truststore
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");
    boolean failureCaught = false;
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      failureCaught = true;
    }
    assertEquals(failureCaught, true);
  }

  @Test
  public void testSSLConnectionFailBadPassword() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, tsPath);
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "bad_password");
    boolean failureCaught = false;
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      failureCaught = true;
    }
    assertEquals(failureCaught, true);
  }

  @Test
  public void testSSLConnectionFailEmptyTrustStore() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, emptyTSPath);
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");
    boolean failureCaught = false;
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      failureCaught = true;
    }
    assertEquals(failureCaught, true);
  }

  @Test
  public void testSSLQuery() throws Exception {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.ENABLE_TLS, "true");
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PATH, tsPath);
    connectionProps.setProperty(DrillProperties.TRUSTSTORE_PASSWORD, "drill123");
    try {
      updateClient(connectionProps);
    } catch (Exception e) {
      TestCase.fail( new StringBuilder()
          .append("SSL Connection failed with exception [" )
          .append( e.getMessage() )
          .append("]")
          .toString());
    }
    test("SELECT * FROM cp.`region.json`");
  }

}
