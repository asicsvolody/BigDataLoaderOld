package ru.yakimov.HDFSConfXML;

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

public class HDFSConfXmlLoader {
    private static final String CONFIGURATION = "configuration";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String TEMP_DIR = "tempDir";
    private static final String JOBS_DIR = "jobsDir";






    @SuppressWarnings({"unchecked", "null", "ConstantConditions"})
    public static HDFSConfiguration readConfig (String configFile) {
        HDFSConfiguration resConfig = null;
        try {

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(configFile);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            HDFSConfiguration config = null;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(CONFIGURATION)) {
                        config = new HDFSConfiguration();
                        Iterator<Attribute> attributes = startElementConf.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();

                            if (attribute.getName().toString().equals(HOST)) {
                                config.setHost(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(PORT)) {
                                config.setPort(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(TEMP_DIR)) {
                                config.setTempDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(JOBS_DIR)) {
                                config.setJobsDir(attribute.getValue());
                            }
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(HOST)) {
                            event = eventReader.nextEvent();
                            config.setHost(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(PORT)) {
                            event = eventReader.nextEvent();
                            config.setPort(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(TEMP_DIR)) {
                            event = eventReader.nextEvent();
                            config.setTempDir(event.asCharacters().getData());
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

//    public static void main(String[] args) {
//        System.out.println(new HDFSConfXmlLoader().readConfig("conf.xml").toString());
//
//    }


}
