import generated.Node;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.bind.JAXBException;
import javax.xml.stream.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class JaxbService {

    private static final Integer DEFAULT_BUFFER_LENGTH = 1000;
    private static final Integer CHAR_SIZE = 32;

    private static final Logger logger = LogManager.getLogger(JaxbService.class);

    private static Statistic statistic;
    private static BZip2CompressorInputStream inputStream;
    private static FileOutputStream outputStream;
    private static Long numberOfSymbolsToRead;

    private final String outFilename;

    public JaxbService(String archiveFilename, String outFilename, long numberToRead) throws IOException {
        inputStream = getInputStream(archiveFilename);
        outputStream = getOutputStream(outFilename);
        this.outFilename = outFilename;
        numberOfSymbolsToRead = numberToRead;
    }

    public Statistic getStatistic() {
        try {
            statistic = calcStatistic();
            return statistic;
        } catch (Exception exception) {
            logger.error(exception);
            return null;
        }
    }

    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            statistic = calcStatistic();
            long endTime = System.currentTimeMillis();

            logger.info("Time taken for calc statistic: " + (endTime - startTime));
            logger.info("Speed: " +  1000.0f * inputStream.getBytesRead() / (endTime - startTime) / 1024 / 1024);

            saveStatisticToFile();
            logger.info("Statistic saved");
        } catch (Exception exception){
            logger.error(exception.getMessage(), exception);
        }
    }

    private static Statistic calcStatistic() throws XMLStreamException, JAXBException {
        Map<String, Integer> userChanges = new HashMap<>();
        Map<String, Integer> nodesIds = new HashMap<>();
        PartialUnmarshaller<Node> nodeReader = new PartialUnmarshaller<>(inputStream, Node.class);

        for (Node node : nodeReader) {
            if (inputStream.getBytesRead() > numberOfSymbolsToRead) break;

            if (node.getChangeset() != null)
                userChanges.merge(node.getUser(), 1, Integer::sum);

            nodesIds.merge(node.getUid().toString(), 1, Integer::sum);
        }

        return new Statistic(userChanges, nodesIds);
    }

    private void saveStatisticToFile() throws XMLStreamException, TransformerException, IOException {
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = outputFactory.createXMLStreamWriter(outputStream);

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

        xmlStreamWriter.writeStartDocument();
        xmlStreamWriter.writeStartElement("statistic");

        xmlStreamWriter.writeStartElement("users_changes");
        for (Map.Entry<String, Integer> entry : statistic.usersChanges.entrySet()) {
            xmlStreamWriter.writeStartElement("entry");
            xmlStreamWriter.writeAttribute("user", entry.getKey());
            xmlStreamWriter.writeAttribute("count", entry.getValue().toString());
            xmlStreamWriter.writeEndElement();
        }
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeStartElement("nodes_IDs");
        for (Map.Entry<String, Integer> entry : statistic.nodesIds.entrySet()){
            xmlStreamWriter.writeStartElement("entry");
            xmlStreamWriter.writeAttribute("user", entry.getKey());
            xmlStreamWriter.writeAttribute("count", entry.getValue().toString());
            xmlStreamWriter.writeEndElement();
        }
        xmlStreamWriter.writeEndElement();

        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndDocument();

        xmlStreamWriter.close();

        String xml = outputStream.toString();
        String prettyPrintXML = formatXML(xml);
        Files.writeString(Paths.get(outFilename),
                prettyPrintXML, StandardCharsets.UTF_8);
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

    private static BZip2CompressorInputStream getInputStream(String inputFileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(inputFileName);

        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, CHAR_SIZE * DEFAULT_BUFFER_LENGTH);
        return new BZip2CompressorInputStream(bufferedInputStream);
    }

    private static FileOutputStream getOutputStream(String outputFileName) throws IOException {
        File file = new File(outputFileName);
        return new FileOutputStream(file);
    }
}

