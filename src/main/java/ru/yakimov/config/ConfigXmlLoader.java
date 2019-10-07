/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import ru.yakimov.Assets;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Iterator;

public class ConfigXmlLoader {

    private static final String CONFIGURATION = "configuration";

    private static final String JOB_CLASS_NAME = "jobClassName";
    private static final String JOB_DIR_TO = "jobDirTo";
    private static final String PARTITION = "partition";
    private static final String MYSQL_CONF = "mysqlConf";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String TEMP_DIR = "tempDir";
    private static final String JOBS_DIR = "jobsDir";
    private static final String LOGS_DIR = "logsDir";



    private static final String CONF_TARGET = "confTarget";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PRIMARY_KEY = "primaryKey";

    public static AppConfiguration readConfigApp(String configFile) {
        AppConfiguration resConfig = null;
        try {

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(configFile);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            AppConfiguration config = null;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(CONFIGURATION)) {
                        config = new AppConfiguration();
                        Iterator<Attribute> attributes = startElementConf.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();

                            if (attribute.getName().toString().equals(HOST)) {
                                config.setHdfsHost(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(PORT)) {
                                config.setHdfsPort(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(TEMP_DIR)) {
                                config.setTmpDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(JOBS_DIR)) {
                                config.setJobsDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(LOGS_DIR)) {
                                config.setLogsDir(attribute.getValue());
                            }
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(HOST)) {
                            event = eventReader.nextEvent();
                            config.setHdfsHost(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(PORT)) {
                            event = eventReader.nextEvent();
                            config.setHdfsPort(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(TEMP_DIR)) {
                            event = eventReader.nextEvent();
                            config.setTmpDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(JOBS_DIR)) {
                            event = eventReader.nextEvent();
                            config.setJobsDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(LOGS_DIR)) {
                            event = eventReader.nextEvent();
                            config.setLogsDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (startElementConf.getName().getLocalPart().equals(MYSQL_CONF)) {
                            addMysqlConfig(config, startElementConf);
                        }
                    }



                }
                if(event.isEndElement()){
                    EndElement endElement = event.asEndElement();
                    if(endElement.getName().getLocalPart().equals(CONFIGURATION)){
                        resConfig = config;
                        break;
                    }
                }

            }

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return resConfig;
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public static JobConfiguration readConfJob(String configFile){
        JobConfiguration resConfig = null;
        try{
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(configFile);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            JobConfiguration config = null;
            while(eventReader.hasNext()){
                XMLEvent event = eventReader.nextEvent();
                if(event.isStartElement()){
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(CONFIGURATION)) {
                        config = new JobConfiguration();
                        config.setJobFile(configFile);
                        config.setJobIdentifier(createJobNameFromPath(configFile)+"_"+getFullTime());
                        config.setJobTmpDir(Assets.getInstance().getConf().getTmpDir()
                                + Assets.SEPARATOR+config.getJobIdentifier());
                        Iterator<Attribute> attributes = startElementConf.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();

                            if (attribute.getName().toString().equals(JOB_CLASS_NAME)) {
                                config.setJobClassName(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(JOB_DIR_TO)) {
                                config.setJobDirTo(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(PARTITION)) {
                                config.setPartition(attribute.getValue());
                            }
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(JOB_CLASS_NAME)) {
                            event = eventReader.nextEvent();
                            config.setJobClassName(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(JOB_DIR_TO)) {
                            event = eventReader.nextEvent();
                            config.setJobDirTo(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(PARTITION)) {
                            event = eventReader.nextEvent();
                            config.setPartition(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (startElementConf.getName().getLocalPart().equals(MYSQL_CONF)) {
                            addMysqlConfig(config, startElementConf);
                        }
                    }


                }
                if(event.isEndElement()){
                    EndElement endElement = event.asEndElement();
                    if(endElement.getName().getLocalPart().equals(CONFIGURATION)){
                        resConfig = config;
                        break;
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resConfig;
    }

    public static void addMysqlConfig(MySqlConfigMapHaver config, StartElement startElementConf) {
        MysqlConfiguration mysqlConf = new MysqlConfiguration();
        String target = null;
        Iterator<Attribute> attributes = startElementConf.getAttributes();
        while (attributes.hasNext()) {
            Attribute attribute = attributes.next();
            if (attribute.getName().toString().equals(CONF_TARGET)) {
                target = attribute.getValue();
            }
            if (attribute.getName().toString().equals(HOST)) {
                mysqlConf.setHost(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PORT)) {
                mysqlConf.setPort(attribute.getValue());
            }
            if (attribute.getName().toString().equals(USER)) {
                mysqlConf.setUser(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PASSWORD)) {
                mysqlConf.setPassword(attribute.getValue());
            }
            if (attribute.getName().toString().equals(SCHEMA)) {
                mysqlConf.setSchema(attribute.getValue());
            }
            if (attribute.getName().toString().equals(TABLE)) {
                mysqlConf.setTable(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PRIMARY_KEY)) {
                mysqlConf.setPrimaryKey(attribute.getValue());
            }
        }
        if (target != null && config != null) {
            config.setMysqlConf(target, mysqlConf);
        }
    }

    private static String getFullTime(){
        return getTimeNumbersOnly(LocalDateTime.now().toString());
    }

    private static String getTimeNumbersOnly(String string){
        StringBuilder sb = new StringBuilder();
        for (char c : string.toCharArray()) {
           if(c >= '0' && c<='9'){
               sb.append(c);
           }
        }
        return sb.toString();
    }

    private static String createJobNameFromPath(String path){
        String[] words = path.split(Assets.SEPARATOR);
        return words[words.length-1].split("\\.",2)[0];
    }


}
