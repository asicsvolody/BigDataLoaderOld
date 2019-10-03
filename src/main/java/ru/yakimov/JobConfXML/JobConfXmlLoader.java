package ru.yakimov.JobConfXML;

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
import java.util.Iterator;

public class JobConfXmlLoader {
    private static final String CONFIGURATION = "configuration";

    private static final String JOB_CLASS_NAME = "jobClassName";
    private static final String HDFS_DIR_TO = "hgfsDirTo";
    private static final String PARTITION = "partition";
    private static final String MYSQL_CONF = "mysqlConf";


    private static final String CONF_TARGET = "confTarget";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PRIMARY_KEY = "primaryKey";


    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public static JobConfiguration readConf(String configFile){
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
                        Iterator<Attribute> attributes = startElementConf.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();

                            if (attribute.getName().toString().equals(JOB_CLASS_NAME)) {
                                config.setJobClassName(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(HDFS_DIR_TO)) {
                                config.setHdfsDirTo(attribute.getValue());
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
                        if (event.asStartElement().getName().getLocalPart().equals(HDFS_DIR_TO)) {
                            event = eventReader.nextEvent();
                            config.setHdfsDirTo(event.asCharacters().getData());
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
        }
        return resConfig;
    }
}
