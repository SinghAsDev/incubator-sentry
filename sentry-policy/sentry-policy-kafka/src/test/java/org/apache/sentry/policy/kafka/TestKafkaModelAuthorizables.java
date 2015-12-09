/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.kafka;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import org.apache.sentry.core.model.kafka.Server;
import org.apache.sentry.policy.kafka.KafkaModelAuthorizables;
import org.junit.Test;

public class TestKafkaModelAuthorizables {

  @Test
  public void testServer() throws Exception {
    Server server1 = (Server)KafkaModelAuthorizables.from("SERVER=server1");
    assertEquals("server1", server1.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoKV() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("nonsense"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("=server1"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("SERVER="));
  }

  @Test
  public void testNotAuthorizable() throws Exception {
    assertNull(KafkaModelAuthorizables.from("k=v"));
  }
}
