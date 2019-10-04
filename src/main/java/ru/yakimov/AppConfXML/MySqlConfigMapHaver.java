package ru.yakimov.AppConfXML;

import ru.yakimov.JobConfXML.MysqlConfiguration;

public interface MySqlConfigMapHaver {

    void setMysqlConf(String target, MysqlConfiguration mysqlConfig);

}
