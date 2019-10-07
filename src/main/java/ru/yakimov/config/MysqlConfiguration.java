/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import java.util.ArrayList;
import java.util.Iterator;

public class MysqlConfiguration {
    private String host;
    private String port;
    private String user;
    private String password;
    private String schema;
    private String table;
    private ArrayList<String> primaryKeys;

    public MysqlConfiguration() {
        this.primaryKeys = new ArrayList<>();
    }


    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKeys.add(primaryKey);
    }


    public Iterator<String> getPrimaryKeys() {
        return primaryKeys.iterator();
    }

    @Override
    public String toString() {
        return "MysqlConfiguration{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", primaryKeys=" + primaryKeys.toString() +
                '}';
    }
}
