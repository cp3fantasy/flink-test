package com.zz.flink.cdc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UserDbWriter {

    static final String DB_URL = "jdbc:mysql://localhost/flink_test?serverTimezone=GMT%2B8";
    static final String USER = "root";
    static final String PASS = "anguo8098";

    private Connection conn;

    public UserDbWriter() {
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new UserDbWriter().run();
    }

    private void run() {
        Set<String> set = new HashSet<>();
        try {
            while (true) {
                String user = "hudi" + (new Random().nextInt(1000) + 1000);
                int level = new Random().nextInt(10);
                BigDecimal amount = new BigDecimal(new Random().nextDouble());
                String sql;
                PreparedStatement statement = null;
                if (!set.contains(user)) {
                    set.add(user);
                    sql = "insert into user_info(userId,level,amount) values(?,?,?)";
                    statement =
                            conn.prepareStatement(sql);
                    statement.setString(1, user);
                    statement.setInt(2, level);
                    statement.setBigDecimal(3, amount);
                } else {
                    sql = "update user_info set level=? and amount=? where userId=?";
                    statement =
                            conn.prepareStatement(sql);
                    statement.setInt(1, level);
                    statement.setBigDecimal(2, amount);
                    statement.setString(3, user);
                }
                System.out.println(sql);
                statement.execute();
                Thread.sleep(10);
            }
        } catch (Exception e) {
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

}
