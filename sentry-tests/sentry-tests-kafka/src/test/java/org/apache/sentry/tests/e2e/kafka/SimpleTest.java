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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.sentry.kafka.authorizer.SentryKafkaAuthorizer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class SimpleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTest.class);
    private static String BOOTSTRAP_SERVERS = null;
    private static KafkaTestServer KAFKA_TEST_SERVER = null;

    @BeforeClass
    public static void setUp() throws Exception {
        // Workaround for SentryKafkaAuthorizer to be added to classpath
        Class.forName("org.apache.sentry.kafka.authorizer.SentryKafkaAuthorizer");

        KAFKA_TEST_SERVER = new KafkaTestServer();
        KAFKA_TEST_SERVER.start();
        BOOTSTRAP_SERVERS = KAFKA_TEST_SERVER.getBootstrapServers();
    }

    @AfterClass
    public static void tearDown() {
        KAFKA_TEST_SERVER.shutdown();
    }

    @Test
    public void testProduceConsumeForSuperuser() throws Exception {
        testProduceAndConsume("test", "test");
    }

    @Test
    public void testProduceConsumeForUser1() throws Exception {
        testProduceAndConsume("user1", "user1");
    }

    private void testProduceAndConsume(String producerUser, String consumerUser) throws Exception {
        final KafkaProducer<String, String> kafkaProducer = createKafkaProducer(producerUser);
        final KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(consumerUser);

        final String topic = "t1";
        final String msg = "message1";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, msg);
        kafkaProducer.send(producerRecord).get();
        LOGGER.debug("Sent message: " + producerRecord);

        kafkaConsumer.subscribe(Collections.singletonList(topic));
        waitTillTrue("Did not receive expected message.", 60, 2, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                if (records.isEmpty())
                    LOGGER.debug("No record received from consumer.");
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().equals(msg)) {
                        return true;
                    }
                    LOGGER.debug("Received message: " + record);
                }
                return false;
            }
        });

        kafkaProducer.close();
        kafkaConsumer.close();
    }

    private KafkaProducer<String, String> createKafkaProducer(String user) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SentryKafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".keystore.jks").getPath());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, user + "-ks-passwd");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, user + "-key-passwd");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".truststore.jks").getPath());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, user + "-ts-passwd");

        return new KafkaProducer<String, String>(props);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String user) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SentryKafkaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".keystore.jks").getPath());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, user + "-ks-passwd");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, user + "-key-passwd");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".truststore.jks").getPath());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, user + "-ts-passwd");

        return new KafkaConsumer<String, String>(props);
    }

    /**
     * Wait for a condition to succeed up to specified time.
     * @param failureMessage Message to be displayed on failure.
     * @param maxWaitTime Max waiting time for success in seconds.
     * @param loopInterval Wait time between checks in seconds.
     * @param testFunc Check to be performed for success, should return boolean.
     * @throws Exception
     */
    protected static void waitTillTrue(
        String failureMessage, long maxWaitTime, long loopInterval, Callable<Boolean> testFunc)
        throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= maxWaitTime * 1000L) {
            if(testFunc.call()) {
                return; // Success
            }
            Thread.sleep(loopInterval * 1000L);
        }

        Assert.fail(failureMessage);
    }
}