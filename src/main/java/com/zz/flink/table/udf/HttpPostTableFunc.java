package com.zz.flink.table.udf;

import com.alibaba.fastjson.JSON;
import okhttp3.*;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HttpPostTableFunc extends TableFunction<Row> {

    private static final MediaType mediaType
            = MediaType.parse("application/json; charset=utf-8");

//    private transient Histogram histogram;

    private transient OkHttpClient client;

    @Override
    public void open(FunctionContext context) throws Exception {
//        new com.codahale.metrics.Histogram();
//        context.getMetricGroup().histogram("http",);
        client = new OkHttpClient.Builder().
                connectTimeout(2, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS).build();
    }

    @FunctionHint(output = @DataTypeHint("ROW<resp STRING, status STRING>"))
    public void eval(String url, Map<String, String> headers, Map<String, String> params) {
        Request request = buildRequest(url, headers, params);
        try {
            Response response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                String respStr = response.body().string();
                this.collect(Row.of(respStr, "success"));
            } else {
                this.collect(Row.of(null, String.valueOf(response.code())));
            }
        } catch (IOException e) {
//            e.printStackTrace();
            this.collect(Row.of(null,e.getClass().getName()));
        }
    }

    private Request buildRequest(String url, Map<String, String> headers, Map<String, String> params) {
        Request.Builder builder = new Request.Builder().url(url);
        if(headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                builder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        String json = JSON.toJSONString(params);
        RequestBody body = RequestBody.create(mediaType, json);
        builder.post(body);
        return builder.build();
    }
}
