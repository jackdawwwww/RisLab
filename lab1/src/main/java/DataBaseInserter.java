import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.postgresql.Driver;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.util.HashMap;
import java.util.Map;
import generated.Node;

public class DataBaseInserter {

    private static final Integer DEFAULT_BUFFER_LENGTH = 5000000;
    private static final Integer CHAR_SIZE = 32;
    private static final Logger logger = LogManager.getLogger(JaxbService.class);
    private Connection connection;
    private static BZip2CompressorInputStream inputStream;
    private final long numberOfSymbolsToRead;
    private final String inputFileName;


    public DataBaseInserter(String inputFileName, long numberOfSymbolsToRead,
                            String database, String userName, String password) {
        this.inputFileName = inputFileName;
        this.numberOfSymbolsToRead = numberOfSymbolsToRead;
        try {
            DriverManager.registerDriver(new Driver());
            this.connection =
                    DriverManager.getConnection(database, userName, password);
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    void insertToDatabaseSimple(String tableName, String methodName) {
        Map <String, DataLoader> loaders = new HashMap<>();
        loaders.put("node insert", new NodeStmtImport());
        loaders.put("node prepared", new NodePreparedStmtImport(false));
        loaders.put("node batch", new NodePreparedStmtImport(true));
        loaders.put("node_json insert", new NodeJSONStmtImport());
        loaders.put("node_json prepared", new NodeJSONPreparedStmtImport(false));
        loaders.put("node_json batch", new NodeJSONPreparedStmtImport(true));

        try {
            inputStream = getInputStream();

            connection.setAutoCommit(false);

            PartialUnmarshaller<Node> nodeReader = new PartialUnmarshaller<>(inputStream, Node.class);
            DataLoader loader = loaders.get(tableName + ' ' + methodName);
            loader.cleanup(connection);
            connection.commit();
            long startTime = System.currentTimeMillis();
            int count = loader.loadData(nodeReader, connection, () -> inputStream.getBytesRead() > numberOfSymbolsToRead);
            connection.commit();
            long endTime = System.currentTimeMillis();

            logger.info("Time token: " + (endTime - startTime));
            logger.info("Speed: " +  1000.0f * inputStream.getBytesRead() / (endTime - startTime) / 1024 / 1024 +
                    "; Node count: " + count +
                    "; Node speed: " + 1000.0f * count / (endTime - startTime));
        } catch (IOException | SQLException | JAXBException | XMLStreamException exception){
            logger.error(exception);
        }
    }
    private BZip2CompressorInputStream getInputStream() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(inputFileName);

        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, CHAR_SIZE * DEFAULT_BUFFER_LENGTH);
        return new BZip2CompressorInputStream(bufferedInputStream);
    }
}
