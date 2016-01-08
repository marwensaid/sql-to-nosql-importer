package orange.save.export.sql.mongo;

import com.mongodb.*;
import orange.save.export.sql.model.NoSQLWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class MongoWriter extends NoSQLWriter {
	
	DBCollection collection = null;
	
	private DB db;
	
	public DB getDB() {
		return db;
	}
	
	private static Log log = LogFactory.getLog(MongoWriter.class);
	
	@Override
	public void initConnection(ResourceBundle resourceBundle) throws UnknownHostException, MongoException {
		if (resourceBundle.getString("mongo.useAuth").equalsIgnoreCase("true")) {
			initConnection(resourceBundle.getString("mongo.host"), resourceBundle.getString("mongo.db") ,
					resourceBundle.getString("mongo.user"), resourceBundle.getString("mongo.password"));
		} else {
			initConnection( resourceBundle.getString("mongo.host"), resourceBundle.getString("mongo.db") );
		}
		initCollection(resourceBundle.getString("mongo.collection"));
	}
	
	private void initCollection(String name) {
		collection = getDB().getCollection(name);
	}
	
	public void initConnection(String url, String dbName) throws UnknownHostException, MongoException {
		Mongo mongo = new Mongo(url);
		db = mongo.getDB(dbName);
	}
	public void initConnection(String url, String dbName, String user, String password) throws UnknownHostException, MongoException {
		Mongo mongo = new Mongo(url);
		db = mongo.getDB(dbName);
		
		if (!db.authenticate(user, password.toCharArray())) {
			log.error("Couldn't Authenticate MongoDB !");
			throw new MongoException("Couldn't Authenticate !");
		}
	}
	
	@Override
	public void writeToNoSQL(List<Map<String, Object>> entityList) {
		List<DBObject> list = new ArrayList<DBObject>();
		DBObject object = null;
		for (int i = 0; i < entityList.size(); i++) {
			object = new BasicDBObject(entityList.get(i));
			list.add(object);
		}
		if (list.size() > 0) {
			long  t1 = System.currentTimeMillis();
			collection.insert(list);
			long t2 = System.currentTimeMillis();
			log.info("Time taken to Write "+ list.size() + " documents to NoSQL :" + ((t2-t1))  + " ms");
		}
			
}
}
