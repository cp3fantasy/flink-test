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

package com.zz.flink.jobs.loan;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlMsgProducer {

    private static final Logger log = LoggerFactory.getLogger(ControlMsgProducer.class);

    private static final int MESSAGE_NUM = 10;

    // Producer config
    private static final String NAME_SERVER_ADDR = "localhost:9876";
    private static final String PRODUCER_GROUP = "CTRL_PRODUCER";
    private static final String TOPIC = "BLOAN-RCPM_EVENT";
    private static final String CTRL_TAGS = "black_list_ctrl";


    public static void main(String[] args) {
        DefaultMQProducer producer =
                new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < MESSAGE_NUM; i++) {
            String content = buildMsg(i);
            Message msg = new Message(TOPIC, CTRL_TAGS, content.getBytes());
            try {
                SendResult sendResult = producer.send(msg);
                assert sendResult != null;
                System.out.printf(
                        "send result: %s %s\n",
                        sendResult.getMsgId(), sendResult.getMessageQueue().toString());
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
                log.info("send message failed. {}", e.toString());
            }
        }
        producer.shutdown();
    }

    private static String buildMsg(int i) {
        ControlMsg msg = new ControlMsg();
        msg.setAccountNo("acct" + i % 3);
        msg.setCustomerName("cust" + i % 3);
        msg.setOperateType("D");
        return JSON.toJSONString(msg);
    }
}
