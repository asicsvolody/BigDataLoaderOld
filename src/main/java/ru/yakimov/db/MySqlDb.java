/**
 * Класс для обслуживания подключения к БД Mysql
 * БД Mysql хранит информацию необходимссую для работы, логи
 * использует общее статическое подключение (singleton)
 */
package ru.yakimov.db;

import ru.yakimov.JobConfXML.MysqlConfiguration;

import java.sql.*;
import java.util.ArrayList;

public class MySqlDb {

    private MysqlConfiguration config;

    public static String dbUrl;

    public static String dbUser;

    public static String dbPass;

    private static Connection conn;

    private final static String JDBC = "com.mysql.jdbc.Driver";

    public static Boolean dbDebug = false;

    public static boolean isDbConnected() {
        final String CHECK_SQL_QUERY = "SELECT 1";
        boolean isConnected = false;
        try {
            conn.prepareStatement(CHECK_SQL_QUERY).execute();
            isConnected = true;
        }
        catch (SQLException | NullPointerException e)
        {
            if (dbDebug)
            {
                System.err.println("Connection is closed!");
                System.err.println(e.getMessage());

            }
        }
        return isConnected;
    }

    public MySqlDb() {
    }

    /**
     * Получить connection (singleton)
     * @return
     * @throws Exception
     */

    public static Connection getConnection() throws Exception {

        if (conn == null || (conn != null && !isDbConnected())) {
            try {
                Class.forName(JDBC);
                conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            } catch (Exception e) {
                System.out.println("Ошибка подключения к БД MySQL!");
                throw e;
            }
            conn.setAutoCommit(false);

            // Установка параметров
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("set session wait_timeout = 201600");
            stmt.close();

            return conn;
        }
        return conn;
    }



    /**
     * Получить результат выполнения запроса sql(первая запись первое поле)
     *
     * @param sql
     * @return
     * @throws SQLException
     */

    public static String getSqlResult(String sql) throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        String res = "";
        while (rs.next()) {
            res = rs.getString(1);
        }
        rs.close();
        stmt.close();
        return res;
    }

    /**
     * Выполнить sql команду
     * @param sql
     * @throws SQLException
     */

    public static void execSQL(String sql) throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
    }

    /**
     * Получить результат выполнения запроса sql (коллекция из записей первой
     * колонки результата)
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public static ArrayList<String> getSqlResults(String sql)
            throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ArrayList<String> res = new ArrayList<String>();
        while (rs.next()) {
            res.add(rs.getString(1));
        }
        rs.close();
        stmt.close();
        return res;
    }

    /**
     * Инициализация соединения (singleton)
     *
     *
     * @param config
     *      конфигурация Mysql
     * @return
     * @throws Exception
     */
    public static Connection initConnection(MysqlConfiguration config) throws Exception {
        return initConnection(config, false);
    }

    /**
     * Инициализация соединения (singleton)
     *
     * @param config
     *      конфигурация Mysql
     * @param debug
     * @return
     * @throws Exception
     */
    public static Connection initConnection(MysqlConfiguration config, Boolean debug) throws Exception {
        dbUrl = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL",config.getHost(),config.getPort(),config.getSchema());
        dbUser = config.getUser();
        dbPass = config.getPassword();
        if (debug == null) {
            dbDebug = false;
        }
        dbDebug = debug;

        return getConnection();
    }


    /**
     * Закрыть соединение (singleton)
     *
     * @throws SQLException
     */
    public static void closeConnection() throws SQLException {
        if (conn == null) {
            return;
        }
        conn.commit();
        conn.close();
    }






}
