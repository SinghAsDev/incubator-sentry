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

public class SentryKafkaAuthorizerTest {
    /*
      var authorizer = new SentryKafkaAuthorizer
  val testHostName1 = InetAddress.getByName("server1")
  val testHostName2 = InetAddress.getByName("server2")
  val resourceName: String = Resource.ClusterResourceName
  val clusterResource: Resource = new Resource(Cluster, resourceName)
  val topic1Resource: Resource = new Resource(Topic, "t1");
  val topic2Resource: Resource = new Resource(Topic, "t2");
  var config: KafkaConfig = null

  @Before
  def setUp() {
    val props = new Properties
    val sentry_site_path = classOf[SentryKafkaAuthorizerTest].getClassLoader.getResource(KafkaAuthConf.AUTHZ_SITE_FILE).getPath
    // Kafka check this prop when creating a config instance
    props.put("zookeeper.connect", "test")
    props.put("sentry.kafka.site.url", "file://" + sentry_site_path)

    config = KafkaConfig.fromProps(props)
    authorizer.configure(config.originals)
  }

  @Test
  def testAdmin() {

    val admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin")
    val host1Session = new Session(admin, testHostName1)
    val host2Session = new Session(admin, testHostName2)

    assertTrue("Test failed.", authorizer.authorize(host1Session,Create,clusterResource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Describe,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host1Session,ClusterAction,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host1Session,Read,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Write,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Create,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Delete,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Alter,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Describe,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,ClusterAction,topic1Resource));

    assertTrue("Test failed.", authorizer.authorize(host2Session,Create,clusterResource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Describe,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host2Session,ClusterAction,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host2Session,Read,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Write,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Create,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Delete,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Alter,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,Describe,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host2Session,ClusterAction,topic1Resource));
  }

  @Test
  def testSubAdmin() {

    val admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "subadmin")
    val host1Session = new Session(admin, testHostName1)
    val host2Session = new Session(admin, testHostName2)

    assertTrue("Test failed.", authorizer.authorize(host1Session,Create,clusterResource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Describe,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host1Session,ClusterAction,clusterResource))
    assertTrue("Test failed.", authorizer.authorize(host1Session,Read,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Write,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Create,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Delete,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Alter,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,Describe,topic1Resource));
    assertTrue("Test failed.", authorizer.authorize(host1Session,ClusterAction,topic1Resource));

    assertFalse("Test failed.", authorizer.authorize(host2Session,Create,clusterResource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Describe,clusterResource))
    assertFalse("Test failed.", authorizer.authorize(host2Session,ClusterAction,clusterResource))
    assertFalse("Test failed.", authorizer.authorize(host2Session,Read,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Write,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Create,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Delete,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Alter,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,Describe,topic1Resource));
    assertFalse("Test failed.", authorizer.authorize(host2Session,ClusterAction,topic1Resource));

  }
     */
}
