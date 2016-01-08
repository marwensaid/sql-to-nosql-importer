package orange.save.export.sql.couch;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import orange.save.export.sql.model.NoSQLWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class CouchWriter extends NoSQLWriter {
  
  DefaultHttpClient httpclient = null;
  
  private static Log log = LogFactory.getLog(CouchWriter.class);
  
  public String url;
  
  @Override
  public void initConnection(ResourceBundle resourceBundle) throws
          IOException {
    url = "http://" + resourceBundle.getString("couch.host") + ":"
        + resourceBundle.getString("couch.port") + "/" + resourceBundle.getString("couch.db")
        + "/_bulk_docs";
  }
  
  @Override
  public void writeToNoSQL(List<Map<String,Object>> entityList)
      throws IOException, HttpException {
    
    JSONArray jsonArray = JSONArray.fromObject(entityList);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("docs", jsonArray);
    
    if (jsonArray.size() > 0) {
      long t1 = System.currentTimeMillis();
      post(jsonObject.toString());
      long t2 = System.currentTimeMillis();
      log.info("Time taken to Write " + jsonArray.size()
          + " documents to CouchDB :" + ((t2 - t1)) + " ms");
    }
  }
  
  void post(String content) throws  IOException, HttpException {
    HttpPost httpPost = new HttpPost(url);
    if (content != null) {
        httpclient = new DefaultHttpClient();
        HttpEntity entity = new StringEntity(content, ContentType.APPLICATION_JSON);
        httpPost.setEntity(entity);
        httpPost.setHeader(new BasicHeader("Content-Type", "application/json"));
        HttpResponse response = httpclient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED)
          throw new HttpException(response.getStatusLine().toString());
    }
  }
  
}
