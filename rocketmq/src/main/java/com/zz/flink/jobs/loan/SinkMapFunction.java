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
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.Message;

public class SinkMapFunction extends ProcessFunction<ControlMsg, Message> {

    private String topic;

    private String tag;

    public SinkMapFunction() {
    }

    @Override
    public void processElement(ControlMsg msg, Context ctx, Collector<Message> out) throws Exception {
        String msgString = JSON.toJSONString(msg);
        Message message = new Message(topic, tag, msgString.getBytes());
        out.collect(message);
    }

    public SinkMapFunction(String topic, String tag) {
        this.topic = topic;
        this.tag = tag;
    }

}
