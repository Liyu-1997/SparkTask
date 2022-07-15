package com.sg.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 待完善，用于写PostgreSQL客户端连接PostgreSQL进行crud
 */
public class PostgreSQLClient {

    public void getConn(String url, String user, String password) throws SQLException {
        final Connection connection = DriverManager.getConnection(url, user, password);
    }


}
