package com.zz.flink.cdc;

import java.sql.*;

public class PvDataSimulator {

    static final String DB_URL = "jdbc:mysql://localhost/flink_test";
    static final String USER = "flink";
    static final String PASS = "flink";
    static final String QUERY = "SELECT * FROM pv";

    public static void main(String[] args) {
        // Open a connection
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(QUERY);) {
            // Extract data from result set
            while (rs.next()) {
                System.out.println(rs.getInt(0));
                // Retrieve by column name
//                System.out.print("ID: " + rs.getInt("id"));
//                System.out.print(", Age: " + rs.getInt("age"));
//                System.out.print(", First: " + rs.getString("first"));
//                System.out.println(", Last: " + rs.getString("last"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
