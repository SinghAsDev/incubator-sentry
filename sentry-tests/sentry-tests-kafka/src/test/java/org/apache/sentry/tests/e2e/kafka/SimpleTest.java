/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.kafka;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

public class SimpleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTest.class);
    private static KafkaTestServer KAFKA_TEST_SERVER = null;

    @BeforeClass
    public static void setUp() throws Exception {
        KAFKA_TEST_SERVER = new KafkaTestServer();
        KAFKA_TEST_SERVER.start();
    }

    @AfterClass
    public static void tearDown() {
        KAFKA_TEST_SERVER.shutdown();
    }

    @Test
    public void serverStartAndShutdown() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            LOGGER.info(url.getFile());
        }
        Assert.assertTrue("Success", true);
    }
}