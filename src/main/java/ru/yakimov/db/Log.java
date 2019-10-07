/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Отображение таблицы Mysql SYSTEM_LOG и методы работы с ней
 * Таблица хранит логи работы системы
 */
package ru.yakimov.db;

import java.sql.PreparedStatement;
import java.time.LocalDate;

public class Log {

    /**
     *  enum
     *  Константы уровней логирования
     */

    public enum Level{
        TRACE("TRACE")
        , DEBUG("DEBUG")
        , INFO("INFO")
        , WARNING("WARNING")
        , ERROR("ERROR")
        , FATAL("FATAL");

        public String levelName;

        Level(String levelName) {
            this.levelName = levelName;
        }

        public String getLevelName() {
            return levelName;
        }
    }

    /**
     * Запись сообщения в журнал
     *
     * @param jobIdentifier
     *       Идентификатор задачи
     * @param msg
     *       Сообщение лога
     * @param level
     *        Уровень лога
     * @param doThrow
     * @throws Exception
     */

    public static void write(String jobIdentifier, String msg, Log.Level level,
                             boolean doThrow) throws Exception {
        System.out.println(jobIdentifier+": "+msg);

        String sql = "INSERT INTO SYSTEM_LOG(SYSTEM_LOG_JOB_IDENTIFIER,SYSTEM_LOG_MSG,SYSTEM_LOG_LEVEL,SYSTEM_LOG_DATA)" +
                " VALUES(?,?,?,?)";
        PreparedStatement ps = MySqlDb.getConnection().prepareStatement(sql);
        ps.setString(1, jobIdentifier);
        ps.setString(2, msg.replaceAll("/n",""));
        ps.setString(3, level.getLevelName());
        ps.setString(4, String.valueOf(LocalDate.now()));
        ps.executeUpdate();
        ps.close();
        MySqlDb.getConnection().commit();

        if (doThrow) {
            MySqlDb.closeConnection();
            System.err.println(msg);
            System.exit(-1);
        }
    }

    /**
     * Записать сообщение в журнал.
     * @param jobIdentifier
     *       Идентификатор задачи
     * @param msg
     *       Сообщение лога
     * @throws Exception
     */
    public static void write(String jobIdentifier, String msg) throws Exception {
        write(jobIdentifier,  msg, Level.INFO, false);
    }

    /**
     * Записать сообщение в журнал.
     * @param jobIdentifier
     *       Идентификатор задачи
     * @param msg
     *       Сообщение лога
     * @param level
     * @throws Exception
     */

    public static void write(String jobIdentifier, String msg, Log.Level level)
            throws Exception {
        write(jobIdentifier, msg, level, false);
    }


    /**
     * Записать сообщение в журнал, в конце вызвать исключение и завершить
     * работу
     * @param jobIdentifier
     *       Идентификатор задачи
     * @param msg
     *       Сообщение лога
     * @throws Exception
     */
    public static void raise(String jobIdentifier, String msg) throws Exception {
        write(jobIdentifier,  msg, Level.ERROR, true);
    }

}
