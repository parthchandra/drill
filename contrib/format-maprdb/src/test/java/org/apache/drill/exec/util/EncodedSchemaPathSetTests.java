/**
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
package org.apache.drill.exec.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EncodedSchemaPathSetTests {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final MyRandom RND = new MyRandom(RANDOM_SEED);
  private static final String[] EMPTY_ARRAY = new String[0];

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSimpleSchemaPathSets() {
    testEncoding(new String[] {"a", "b", "c"});
  }

  @Test
  public void testDecodingMixedUnorderedEncodedSets() {
    String[] originalSchemaPaths = new String[] {
        "a", "b", "c", "d", "e",
        "GxLoCim[35044]",
        "pFbPbrIVab.kpth57sho4e06fO4qA195[13205].lU9MCn5sD1zSB[25774][49745]",
        "ndPVzFc[26712].4JJMpR5WKR[60612][24442].0Zxyi.UJ2RBaChsJagno[70059][18841].Yqa.nYi3T9eDJ",
        "Kpq.agRBoXcvBCKGH4RvuJqnSDr8v.GVwFxDK2cR8A[33735][62619]",
        "YiZPUmELB.mgyPiSqeyrr8WtISytEP[56345]",
        "UUWdZWJ[84042].9[40139]",
        "nvmLVKby[14183].mO8Kc2j74[66623][97609][51575].t4S8fxG8IeG2.j9OVRdzyc",
        "IwR[93981].mfDVdsyF.9oi7N3",
        "kYwHiGm[77074].wWElaNRcOpyKqURQ.oOKUk.8",
        "ykV.EzcRES[79911][68381][68508][65057].nT6WWXgnUaaFjZAUr.kTN",
        "EUTcyRKu[31225][88057].d6TDHP8KP7GwWpZmDeKlO[30998][65065][62207].AYHMh5MSu6IM4Tl8NA"};
    String[] unorderedMixedEncodedSchemaPaths = new String[]{
        "a", "b", "c",
        "$$ENC05NIVGEELTNM54VA2KTOFSXS4TSHBLXISKTPF2EKUC3GU3DGNBVLUAFKVKXMRNFOSS3HA2DANBSLUXDSWZUGAYTGOK5ABXH",
        "$$ENC06M3KMKZFWE6K3GE2DCOBTLUXG2TZYJNRTE2RXGRNTMNRWGIZV2WZZG43DAOK5LM2TCNJXGVOS45BUKM4GM6CHHBEWKRZSFZVDST2WKJSH",
        "$$ENC01TGJ42HCQJRHE2VWMJTGIYDKXJONRKTSTKDNY2XGRBRPJJUEWZSGU3TONC5LM2DSNZUGVOQA3TEKBLH",
        "$$ENC11TLIGVGVG5JWJFGTIVDMHBHEC",
        "e",
        "$$ENC02URTDLMZDMNZRGJOS4NCKJJGXAURVK5FVEWZWGA3DCMS5LMZDINBUGJOS4MC2PB4WSLSVJIZF",
        "$$ENC09ONNKE4ACFKVKGG6KSJN2VWMZRGIZD",
        "d",
        "$$ENC03EQTBINUHGSTBM5XG6WZXGAYDKOK5LMYTQOBUGFOS4WLRMEXG4WLJGNKDSZKEJIAEW4DRFZQWOUSCN5MGG5SCI",
        "$$ENC00I54EY32DNFWVWMZVGA2DIXIAOBDGEUDCOJEVMYLCFZVXA5DIGU3XG2DPGRSTAN",
        "$$ENC04NFUOSBUKJ3HKSTRNZJUI4RYOYXEOVTXIZ4EISZSMNJDQQK3GMZTOMZVLVNTMMRWGE4V2ACZNFNFAVL",
        "$$ENC10KXK3HA4DANJXLUXGINSUIREFAOCLKA3UO52XOBNG2RDFJNWE6WZTGA4TSOC5LM3DKMBWGVOVWNRSGIYDOXJOIFMUQ",
        "$$ENC07U6LDABEXOUS3HEZTSOBRLUXG2ZSEKZSHG6KGFY4W62JXJYZQA22ZO5EGSR3NLM3TOMBXGROS452XIVWGCTSSMNHXA6KLOFKVE",
        "$$ENC08UJON5HUWVLLFY4AA6LLKYXEK6TDKJCVGWZXHE4TCMK5LM3DQMZYGFOVWNRYGUYDQXK3GY2TANJXLUXG4VBWK5LVQZ3OKVQWCRTKLJAVK4R"};

    String[] decoded = EncodedSchemaPathSet.decode(unorderedMixedEncodedSchemaPaths);
    assertArrayEquals("Decoded array of schema paths are not same as original", originalSchemaPaths, decoded);
  }

  @Test
  public void testBadEncoding() {
    expectedException.expect(DrillRuntimeException.class);
    expectedException.expectCause(CoreMatchers.isA(IllegalArgumentException.class));

    String[] bad_encoded_set = new String[] {"$$ENC02KDH23HD", "$$ENC010KD52GA", "$$ENC03U6H2KA"};
    EncodedSchemaPathSet.decode(bad_encoded_set);
  }

  @Test
  public void testRadmonSchemaPathSets() {
    final int iterationCount = 100;    // run 100 iterations of this test
    for (int iterations = 0; iterations < iterationCount; iterations++) {
      int schemaPathListSize = 10 + RND.nextLength(90); // between 10 to 100 schema paths, encoded together
      List<String> randomSchemaPathList = new ArrayList<String>(schemaPathListSize);
      for (int i = 0; i < schemaPathListSize; i++) {
        // Next, we build a random schema path, mixing both array and name segments of varying length
        int segmentLen = RND.nextLength(10); // Length of the root name segment
        StringBuilder sb = new StringBuilder(RandomStringUtils.randomAlphabetic(segmentLen));
        int segmentCount = RND.nextLength(10); // number of field segments
        for (int j = 0; j < segmentCount; j++) {
          if (RND.nextBoolean()) { // name segment
            sb.append('.');
            segmentLen = RND.nextLength(25);
            sb.append(RandomStringUtils.randomAlphanumeric(segmentLen));
          } else {
            sb.append('[');
            sb.append(RND.nextInt(100000));
            sb.append(']');
          }
        }
        String randomSchemaPath = sb.toString();
        randomSchemaPathList.add(randomSchemaPath);
      }

      String[] originalSchemaPaths = randomSchemaPathList.toArray(EMPTY_ARRAY);
      try {
        testEncoding(originalSchemaPaths);
      } catch (AssertionError e) {
        System.out.printf("Random number generator was initialized with seed %s.\n", RANDOM_SEED);
        throw e;
      }
    }
  }

  private void testEncoding(String[] originalSchemaPaths) {
    //System.out.println(java.util.Arrays.asList(originalSchemaPaths));
    String[] encodedPaths = EncodedSchemaPathSet.encode(originalSchemaPaths);
    //System.out.println(java.util.Arrays.asList(encodedPaths));
    String[] decodedPath = EncodedSchemaPathSet.decode(encodedPaths);
    //System.out.println(java.util.Arrays.asList(decodedPath));

    assertArrayEquals("Decoded array of schema paths are not same as original", originalSchemaPaths, decodedPath);

    for (int i = 0; i < encodedPaths.length; i++) {
      // validate length
      assertTrue("Encoded identifier's lenght exceeded the maximum value",
          encodedPaths[0].length() <= PlannerSettings.DEFAULT_IDENTIFIER_MAX_LENGTH);
      // encoding prefix
      assertTrue("Encoded identifier does not start with the expected prefix",
          encodedPaths[0].startsWith(EncodedSchemaPathSet.ENC_PREFIX));
      // encoding charset
      assertTrue(String.format("Encoded identifier '%s' contains illegal characters", encodedPaths[0]),
          encodedPaths[0].matches("[A-Z0-9$]*")); // Base32 charset [A-Z2-7] & "$$ENC00...$$ENC99"
    }
  }

  private static class MyRandom extends Random {
    private static final long serialVersionUID = -1106885449127423643L;

    public MyRandom(long currentTimeMillis) {
      super(currentTimeMillis);
    }

    /**
     * Returns a +ve random integer between 1 and {@code max}, both inclusive, value.
     */
    public int nextLength(int max) {
      return 1 + nextInt(max);
    }
  }

}
