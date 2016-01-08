package orange.save.export.sql;

import orange.save.export.sql.couch.CouchWriter;
import orange.save.export.sql.elasticsearch.ElasticSearchWriter;
import orange.save.export.sql.model.DataConfig;
import orange.save.export.sql.model.DataStoreType;
import orange.save.export.sql.model.NoSQLWriter;
import orange.save.export.sql.mongo.MongoWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.naming.directory.InvalidAttributesException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;


public class DataImporter {

    private static Log log = LogFactory.getLog(DataImporter.class);

    private DataConfig config;

    private NoSQLWriter writer;

    private DataStoreType dataStoreType;

    int autoCommitSize = 500;

    int getAutoCommitSize() {
        return autoCommitSize;
    }

    void setAutoCommitSize(int newSize) {
        autoCommitSize = newSize;
    }

    DataConfig getConfig() {
        return config;
    }

    public void setWriter(NoSQLWriter noSQLWriter) {
        this.writer = noSQLWriter;
    }

    public NoSQLWriter getWriter() {
        return writer;
    }

    private void findDataStoreWriter() throws InvalidAttributesException {
        if (getDataStoreType().equals(DataStoreType.MONGO)) {
            writer = new MongoWriter();
        } else if (getDataStoreType().equals(DataStoreType.ES)) {
            writer = new ElasticSearchWriter();
        } else if (getDataStoreType().equals(DataStoreType.COUCH)) {
            writer = new CouchWriter();
        } else {
            throw new InvalidAttributesException("The requested datastore support is not available !.");
        }
    }

    public DataImporter(ResourceBundle resourceBundle) throws Exception {
        dataStoreType = DataStoreType.valueOf(resourceBundle.getString("dataStoreType").toUpperCase());
        findDataStoreWriter();
        getWriter().initConnection(resourceBundle);
    }

    private String getXmlFile(String path) {
        if (new File(path).exists()) {
            return path;
        }
        return Thread.currentThread().getContextClassLoader().getResource(path).getFile();
    }

    public void doDataImport(String path) {
        InputSource file = new InputSource(getXmlFile(path));
        loadDataConfig(file);

        if (config != null) {
            for (DataConfig.Entity e : config.document.entities) {
                Map<String, DataConfig.Field> fields = new HashMap<String, DataConfig.Field>();
                initEntity(e, fields, false);
                identifyPk(e);
            }
            doFullImport();
        } else {
            log.error("Configuration files are missing !");
        }
    }

    private void identifyPk(DataConfig.Entity entity) {

        String schemaPk = "";
        entity.pkMappingFromSchema = schemaPk;

        for (DataConfig.Field field : entity.fields) {
            if (field.getName().equals(schemaPk)) {
                entity.pkMappingFromSchema = field.column;
                break;
            }
        }

    }

    private void loadDataConfig(InputSource configFile) {

        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document;
            try {
                document = documentBuilder.parse(configFile);
            } finally {
                // some XML parsers are broken and don't close the byte stream (but they should according to spec)
                IOUtils.closeQuietly(configFile.getByteStream());
            }

            config = new DataConfig();
            NodeList nodeList = document.getElementsByTagName("dataConfig");
            if (nodeList == null || nodeList.getLength() == 0) {
                log.error("the root node '<dataConfig>' is missing");
                throw new IOException();
            }
            config.readFromXml((Element) nodeList.item(0));
            log.info("Data Configuration loaded successfully");
        } catch (Exception e) {
            log.error(e.getStackTrace());
        }

    }

    private void initEntity(DataConfig.Entity entity,
                            Map<String, DataConfig.Field> fields, boolean docRootFound) {
        entity.allAttributes.put(DATA_SRC, entity.dataSource);

        if (!docRootFound && !"false".equals(entity.docRoot)) {
            entity.isDocRoot = true;
        }

        if (entity.fields != null) {
            for (DataConfig.Field field : entity.fields) {

                fields.put(field.getName(), field);
                field.entity = entity;
                field.allAttributes.put("boost", field.boost.toString());
                field.allAttributes.put("toWrite", Boolean.toString(field.toWrite));
                entity.allFieldsList.add(Collections
                        .unmodifiableMap(field.allAttributes));
            }
        }
        entity.allFieldsList = Collections.unmodifiableList(entity.allFieldsList);
        entity.allAttributes = Collections.unmodifiableMap(entity.allAttributes);

        if (entity.entities == null)
            return;
        for (DataConfig.Entity entity1 : entity.entities) {
            entity1.parentEntity = entity;
            initEntity(entity1, fields, entity.isDocRoot || docRootFound);
        }
    }

    public void doFullImport() {
        try {
            DocBuilder docBuilder = new DocBuilder(this);
            docBuilder.execute();
            getWriter().close();
            log.info("*****  Data import completed successfully. **********");
        } catch (Throwable t) {
            log.error("*****  Data import failed. **********\n Reason is :");
            t.printStackTrace();
        }
    }

    public void setDataStoreType(DataStoreType exportType) {
        this.dataStoreType = exportType;
    }

    public DataStoreType getDataStoreType() {
        return dataStoreType;
    }


    public static final String COLUMN = "column";

    public static final String TYPE = "type";

    public static final String DATA_SRC = "dataSource";

}
