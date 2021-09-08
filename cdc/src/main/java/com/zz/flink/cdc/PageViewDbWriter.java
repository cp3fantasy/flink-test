package com.zz.flink.cdc;

import com.zz.flink.common.model.PageView;
import com.zz.flink.common.simulator.PageViewHandler;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class PageViewDbWriter implements PageViewHandler {

    static final String DB_URL = "jdbc:mysql://localhost/flink_test?serverTimezone=GMT%2B8";
    static final String USER = "root";
    static final String PASS = "anguo8098";

    private Connection conn;

    public PageViewDbWriter() {
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void handle(PageView pageView) {
        try {
            PreparedStatement statement =
                    conn.prepareStatement("insert into pv(pageId,userId,startTime,endTime) values(?,?,?,?)");
            statement.setString(1,pageView.getPageId());
            statement.setString(2,pageView.getUserId());
//            statement.setTimestamp(3,new Timestamp(pageView.getStartTime()));
//            statement.setTimestamp(4,new Timestamp(pageView.getEndTime()));
            statement.setTimestamp(3, toTimestamp(pageView.getStartTime()));
            statement.setTimestamp(4, toTimestamp(pageView.getEndTime()));
            statement.execute();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    public Timestamp toTimestamp(long time){
        Timestamp timestamp =  Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(time),ZoneId.systemDefault()));
        System.out.println(timestamp);
        return timestamp;
    }
}
