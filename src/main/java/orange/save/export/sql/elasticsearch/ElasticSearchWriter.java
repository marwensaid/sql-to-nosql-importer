package orange.save.export.sql.elasticsearch;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import orange.save.export.sql.model.NoSQLWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.FailedCommunicationException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class ElasticSearchWriter extends NoSQLWriter {

    private static Log log = LogFactory.getLog(ElasticSearchWriter.class);

    Client client = null;

    String index_name = null;

    String index_type = null;

    BulkRequestBuilder bulkRequest;

    @Override
    public void initConnection(ResourceBundle resourceBundle) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", resourceBundle.getString("es.cluster.name")).build();
        client = new TransportClient(settings);
        String host[] = StringUtils.split(resourceBundle.getString("es.hosts"), ",");
        for (int i = 0; i < host.length; i++) {
            ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host[i], 9300));
        }
        bulkRequest = client.prepareBulk();
        setIndexProperties(resourceBundle.getString("es.index.name"), resourceBundle.getString("es.index.type"));
    }

    void setIndexProperties(String index_name, String index_type) {
        this.index_name = index_name;
        this.index_type = index_type;
    }

    @Override
    public void writeToNoSQL(List<Map<String, Object>> entityList) {
        JSONArray array = JSONArray.fromObject(entityList);
        for (int i = 0; i < array.size(); i++) {
            IndexRequestBuilder builder = client.prepareIndex(index_name, index_type);
            if (getPrimaryKey() != null)
                builder.setId(((JSONObject) array.get(i)).getString(getPrimaryKey()));
            builder.setSource(array.get(i).toString());
            bulkRequest.add(builder);
        }
        if (bulkRequest.numberOfActions() > 0) {
            long t1 = System.currentTimeMillis();
            ListenableActionFuture<BulkResponse> action = bulkRequest.execute();
            long t2 = System.currentTimeMillis();
            BulkResponse response = action.actionGet();
            for (Iterator<BulkItemResponse> iterator = response.iterator(); iterator.hasNext(); ) {
                BulkItemResponse bulkItemResponse = (BulkItemResponse) iterator.next();
                if (bulkItemResponse.isFailed())
                    throw new FailedCommunicationException("Insertion to ES failed.");
            }
            log.info("Time taken to Write " + bulkRequest.numberOfActions() + " documents to ES :" + ((t2 - t1)) + " ms");
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
