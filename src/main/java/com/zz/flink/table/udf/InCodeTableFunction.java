package com.zz.flink.table.udf;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

public class InCodeTableFunction extends ScalarFunction {

    private transient Map<String,Map<String,String>> codeTableMap;
    private transient ScheduledExecutorService executor;

    private String configUrl;

    private static final MediaType mediaType
            = MediaType.parse("application/json; charset=utf-8");

//    private transient Histogram histogram;

    private transient OkHttpClient client;

    @Override
    public void open(FunctionContext context) throws Exception {
        codeTableMap = new ConcurrentHashMap<>();
        Map<String, String> table = new HashMap<>();
        for(int i=0;i<10;i++){
            table.put("page"+i,i%2==0?"prod1":"prod2");
        }
        codeTableMap.put("pageInfo",table);
        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("CodeTableUpdate");
                return thread;
            }
        });
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateCodeTables();
            }
        },10,10, TimeUnit.SECONDS);
        configUrl = getConfigUrl(context);
        client = new OkHttpClient.Builder().
                connectTimeout(2, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS).build();
    }

    private String getConfigUrl(FunctionContext context) {
        return "http://localhost:9000/customTable/";
    }

    private void updateCodeTables() {

    }

    public String eval(String codeTableName,String code){
          Map<String,String> codeTable =  codeTableMap.computeIfAbsent(codeTableName,
                  new Function<String, Map<String,String>>() {
              @Override
              public Map<String,String> apply(String codeTableName) {
                  return updateCodeTable(codeTableName);
              }
          });
          return codeTable.get(code);
    }

    private Map<String, String> updateCodeTable(String codeTableName) {
        return null;
    }


}
