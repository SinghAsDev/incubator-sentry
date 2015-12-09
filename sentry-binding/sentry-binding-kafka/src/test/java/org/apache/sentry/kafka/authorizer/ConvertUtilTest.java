/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.kafka.authorizer;

public class ConvertUtilTest {
    /*
      @Test
  def testCluster {
    val hostname: String = "localhost"
    val clusterName: String = Resource.ClusterResourceName
    val clusterResource: Resource = new Resource(Cluster, clusterName)
    val authorizables: util.List[Authorizable] = ConvertUtil.convertResourceToAuthorizable(hostname, clusterResource)
    import scala.collection.JavaConversions._
    for (auth <- authorizables) {
      if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.CLUSTER.name)) {
        assertEquals(auth.getName, clusterName)
      } else if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.SERVER.name)) {
        assertEquals(auth.getName, hostname)
      } else {
        fail("Type is unexpect:" + auth.getTypeName)
      }
    }
    assertEquals(authorizables.size, 2)
  }

  @Test
  def testTopic {
    val hostname: String = "localhost"
    val topicName: String = "t1"
    val topicResource: Resource = new Resource(Topic, topicName)
    val authorizables: util.List[Authorizable] = ConvertUtil.convertResourceToAuthorizable(hostname, topicResource)
    import scala.collection.JavaConversions._
    for (auth <- authorizables) {
      if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.TOPIC.name)) {
        assertEquals(auth.getName,topicName)
      } else if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.SERVER.name)) {
        assertEquals(auth.getName,hostname)
      } else {
        fail("Type is unexpect:" + auth.getTypeName)
      }
    }
    assertEquals(authorizables.size, 2)
  }

  @Test
  def testConsumerGroup {
    val hostname: String = "localhost"
    val consumerGroup: String = "g1"
    val consumerGroupResource: Resource = new Resource(Group, consumerGroup)
    val authorizables: util.List[Authorizable] = ConvertUtil.convertResourceToAuthorizable(hostname, consumerGroupResource)
    import scala.collection.JavaConversions._
    for (auth <- authorizables) {
      if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.CONSUMERGROUP.name)) {
        assertEquals(auth.getName,consumerGroup)
      } else if (auth.getTypeName.equalsIgnoreCase(AuthorizableType.SERVER.name)) {
        assertEquals(auth.getName,hostname)
      } else {
        fail("Type is unexpect:" + auth.getTypeName)
      }
    }
    assertEquals(authorizables.size, 2)
  }
     */
}
