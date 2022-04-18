import lombok.SneakyThrows;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PartialUnmarshaller<T> implements AutoCloseable, Iterable<T>, Iterator<T> {
    XMLStreamReader reader;
    Class<T> tClass;
    Unmarshaller unmarshaller;
    boolean endOfNodes;

    public PartialUnmarshaller(InputStream stream, Class<T> tClass) throws JAXBException, XMLStreamException {
        this.tClass = tClass;
        this.unmarshaller = JAXBContext.newInstance(tClass).createUnmarshaller();
        this.reader = XMLInputFactory.newInstance().createXMLStreamReader(stream);
    }

    @SneakyThrows
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            return unmarshaller.unmarshal(reader, tClass).getValue();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SneakyThrows
    public boolean hasNext() {
        try {
            endOfNodes = skipElements();
            return !endOfNodes && reader.hasNext();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void close() throws XMLStreamException {
        reader.close();
    }

    boolean skipElements() throws XMLStreamException {
        int eventType = reader.getEventType();
        boolean noNodes = false;
        while (eventType != XMLStreamConstants.END_DOCUMENT
                && eventType != XMLStreamConstants.START_ELEMENT
                || (eventType == XMLStreamConstants.START_ELEMENT && !"node".equals(reader.getLocalName()))) {
            if (eventType == XMLStreamConstants.START_ELEMENT) {
                String s = reader.getLocalName();
                noNodes = "way".equals(s) || "relation".equals(s);
                if (noNodes) {
                    break;
                }
            }

            eventType = reader.next();
        }
        return noNodes;
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

}
