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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zz.flink.table;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>Usage: <code>StreamSQLExample --planner &lt;blink|flink&gt;</code><br>
 *
 * <p>This example shows how to: - Convert DataStreams to Tables - Register a Table under a name -
 * Run a StreamSQL query on the registered Table
 */
public class StreamSQLExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            System.err.println(
                    "The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
                            + "where planner (it is either flink or blink, and the default is blink) indicates whether the "
                            + "example uses flink planner or blink planner.");
            return;
        }

        DataStream<Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "beer", 2),
                                new Order(1L, "beer", 3),
                                new Order(1L, "apple", 4),
                                new Order(3L, "diaper", 2)));

        DataStream<Order> orderB =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "beer", 2),
                                new Order(1L, "beer", 3),
                                new Order(1L, "apple", 4),
                                new Order(3L, "diaper", 2)));

        // convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        // register DataStream as Table
        tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));
//        System.out.println(tableA);
        // union the two tables
        Table result =
                tEnv.sqlQuery(
                        "SELECT count(1) as cnt,product,user FROM OrderB group by product");
//        tEnv.toRetractStream(result,CountResult.class).print();
//        tEnv.toAppendStream(result,CountResult.class).print();
//        result.execute().print();
//        TableResult sqlResult = tEnv.executeSql("");
//        tEnv.toAppendStream(result, Order.class).print();
        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    public static class CountResult{

        public String product;

        public long cnt;

        public String user;


    }

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;


        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}
