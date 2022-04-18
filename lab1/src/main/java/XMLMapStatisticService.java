import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class XMLMapStatisticService {

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    private final String archivePath;
    private final String outFilename;
    private int numberToRead;

    static Logger logger = LogManager.getLogger(Router.class);
   // private BZip2CompressorInputStream compressorInputStream;

    public XMLMapStatisticService(String archiveFilename, String outFilename, int numberToRead) {
        this.outFilename = outFilename;
        this.numberToRead = numberToRead;
        URL archiveURL = getClass().getClassLoader().getResource(archiveFilename);
        assert archiveURL != null;
        archivePath = archiveURL.getPath();
        logger.info("Input file name = {} output file name = {}", archiveFilename, outFilename);
    }

    public Statistic parse() throws IOException, XMLStreamException {
        Map<String, Integer> usersChanges = new HashMap<>();
        final Map<String, Integer> nodesIds = new HashMap<>();
        try(var compressorInputStream = new BZip2CompressorInputStream(new FileInputStream(archivePath))){
            final XMLEventReader reader = xmlInputFactory.createXMLEventReader(
                    compressorInputStream);

            while (reader.hasNext() && compressorInputStream.getBytesRead() < numberToRead) {
                final XMLEvent nextEvent = reader.nextEvent();

                if (nextEvent.isStartElement()) {
                    final StartElement startElement = nextEvent.asStartElement();


                    if ("node".equals(startElement.getName().getLocalPart())) {//.equals("node")) {
                        final var user = startElement.getAttributeByName(new QName("user")).getValue();
                        final var uid = startElement.getAttributeByName(new QName("uid")).getValue();
                        final var changesetAttribute = startElement.getAttributeByName(
                                new QName("changeset"));

                        if (changesetAttribute != null) {
                            usersChanges.merge(user, 1, Integer::sum);
                        }

                        nodesIds.merge(uid, 1, Integer::sum);
                    }
                }
            }
        }

        usersChanges
                .entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(it -> logger.info(it));

        nodesIds.entrySet().forEach(it -> logger.info(it));

        return new Statistic(usersChanges, nodesIds);
    }

    public void writeStatisticToFile(Statistic statistic) {
        final var output = XMLOutputFactory.newInstance();

        try {
            final var out = new ByteArrayOutputStream();
            final var writer = output.createXMLStreamWriter(out);


            statistic.usersChanges = statistic.usersChanges.entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new
                    ));
            statistic.nodesIds = statistic.nodesIds.entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new
                    ));

            writer.writeStartDocument();
            writer.writeStartElement("statistic");
            writer.writeStartElement("users_changes");

            for (var entry : statistic.usersChanges.entrySet()) {
                writer.writeStartElement("entry");
                writer.writeAttribute("user", entry.getKey());
                writer.writeAttribute("count", entry.getValue().toString());
                writer.writeEndElement();
            }

            writer.writeEndElement();

            writer.writeStartElement("nodes_ids");
            for (var entry : statistic.nodesIds.entrySet()) {
                writer.writeStartElement("entry");
                writer.writeAttribute("user", entry.getKey());
                writer.writeAttribute("count", entry.getValue().toString());
                writer.writeEndElement();
            }

            writer.writeEndElement();
            writer.writeEndElement();
            writer.writeEndDocument();

            writer.flush();
            writer.close();

            String xml = out.toString(StandardCharsets.UTF_8);
            String prettyPrintXML = formatXML(xml);
            Files.writeString(Paths.get(outFilename),
                    prettyPrintXML, StandardCharsets.UTF_8);
        } catch (XMLStreamException | TransformerException | IOException e) {
            e.printStackTrace();
        }
    }

    private static String formatXML(String xml) throws TransformerException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();

        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.STANDALONE, "yes");

        StreamSource source = new StreamSource(new StringReader(xml));
        StringWriter output = new StringWriter();
        transformer.transform(source, new StreamResult(output));

        return output.toString();
    }
}
