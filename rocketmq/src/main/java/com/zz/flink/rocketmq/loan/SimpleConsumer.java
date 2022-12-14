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

package com.zz.flink.rocketmq.loan;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    // Consumer config
    private static final String NAME_SERVER_ADDR =
            "localhost:9876";
    private static final String GROUP = "SIMPLE_CONSUMER";
    private static final String TOPIC = "IDATA-EPLUS_RCPM_EVENT";
    private static final String TAGS = "*";


    public static void main(String[] args) {
        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer(
                        GROUP);
        consumer.setNamesrvAddr(NAME_SERVER_ADDR);

        try {
            consumer.subscribe(TOPIC, TAGS);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(
                (MessageListenerConcurrently)
                        (msgs, context) -> {
                            for (MessageExt msg : msgs) {
                                System.out.printf(
                                        "%s %s %d %s\n",
                                        msg.getMsgId(),
                                        msg.getBrokerName(),
                                        msg.getQueueId(),
                                        new String(msg.getBody()));
                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        });

        try {
            consumer.start();
        } catch (MQClientException e) {
            log.info("send message failed. {}", e.toString());
        }
    }
}
