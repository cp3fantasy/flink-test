package com.zz.flink.jobs.loan;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LoanFlowControlFunction extends KeyedProcessFunction<String, Map, ControlMsg> {

    private MapState<Long, String> state;

    private ValueState<String> customerNameState;

    private int windowMillis;

    private int acctCountLimit;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, String> stateDescriptor =
                new MapStateDescriptor<Long, String>("state",
                        BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        state = getRuntimeContext().getMapState(stateDescriptor);
        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>
                ("customerNameState", BasicTypeInfo.STRING_TYPE_INFO);
        customerNameState = getRuntimeContext().getState(valueStateDescriptor);
        windowMillis = parameters.getInteger(ConfigOptions.key("windowMillis").intType().defaultValue(30 * 1000));
        acctCountLimit = parameters.getInteger(ConfigOptions.key("acctCountLimit").intType().defaultValue(3));
        System.out.println("window:" + windowMillis);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ControlMsg> out) throws Exception {
        long toDeleteTime = -1;
        String toDeleteAcctNo = null;
        Set<String> acctNoSet = new HashSet<>();
        for (Map.Entry<Long, String> entry : state.entries()) {
            if (entry.getKey() <= timestamp - windowMillis) {
                toDeleteTime = entry.getKey();
                toDeleteAcctNo = entry.getValue();
                System.out.println("remove " + entry);
            } else {
                acctNoSet.add(entry.getValue());
            }
        }
        if (toDeleteTime > 0) {
            state.remove(toDeleteTime);
            System.out.println("acctNoSet size:" + acctNoSet.size());
            if (acctNoSet.size() == 2 && !acctNoSet.contains(toDeleteAcctNo)) {
                ControlMsg msg = new ControlMsg();
                msg.setOperateType("D");
                msg.setAccountNo(ctx.getCurrentKey());
//                msg.setCustomerName(oppositeAcctName);
                out.collect(msg);
            }
        }
    }

    @Override
    public void processElement(Map value, Context ctx, Collector<ControlMsg> out) throws Exception {
        Map<String, Object> map = value;
        if (map.containsKey("operateType")) {
            if(map.get("operateType").equals("D")){
                String accountNo = (String) map.get("accountNo");
                System.out.println("reset " + accountNo);
                state.clear();
                customerNameState.clear();
            }
        } else {
            String acctNo = (String) map.get("acctNo");
            String oppositeAcctNo = (String) map.get("oppositeAcctNo");
            String oppositeAcctName = (String) map.get("oppositeAcctName");
//            long timestamp = (long) map.get("timestamp");
            state.put(ctx.timestamp(), acctNo);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + windowMillis);
            Set<String> acctNoSet = new HashSet<>();
//            System.out.println("put "+ acctNo);
            for (Map.Entry<Long, String> entry : state.entries()) {
//                System.out.println("add "+entry.getValue());
                acctNoSet.add(entry.getValue());
            }
            System.out.println(ctx.getCurrentKey() + ":" + acctNoSet.size());
            if (acctNoSet.size() >= 3) {
                ControlMsg msg = new ControlMsg();
                msg.setOperateType("A");
                msg.setAccountNo(oppositeAcctNo);
                msg.setCustomerName(oppositeAcctName);
                customerNameState.update(oppositeAcctName);
                out.collect(msg);
            }
        }
    }

}
