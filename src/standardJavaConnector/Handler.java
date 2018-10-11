package standardJavaConnector;
/* https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html
 * 
 * */

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class Handler {

	public static List<DBConnector> otherConectores = new ArrayList<>();
	public static List<DBConnector> mssqlconectores = new ArrayList<>();
	static JsonObject logAudParams = new JsonObject();
	public static void establishConnectors(String json_path) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		JsonParser parser = new JsonParser();

		JsonObject down_elem = parser.parse(new FileReader(json_path)).getAsJsonObject();
		
		JsonObject elem = down_elem.get("changes").getAsJsonObject();
		logAudParams = down_elem.get("auditory").getAsJsonObject();
				
		for (Entry<String, JsonElement> obj : elem.entrySet()) {
//			System.out.println(obj.getKey());
			JsonObject dbconn = obj.getValue().getAsJsonObject();
//			System.out.println(dbconn.get("host"));
			
			if (dbconn.get("conector").getAsString().equals("MSSQL")) {
				mssqlconectores.add(new DBConnector(obj.getKey(), dbconn.get("host").getAsString(),
						dbconn.get("port").getAsString(), dbconn.get("serverName").getAsString(),
						dbconn.get("dbName").getAsString(), dbconn.get("user").getAsString(),
						dbconn.get("pwd").getAsString(), dbconn.get("conector").getAsString(),
						dbconn.get("query").getAsString()));
			} else {
				otherConectores.add(new DBConnector(obj.getKey(), dbconn.get("host").getAsString(),
						dbconn.get("port").getAsString(), dbconn.get("serverName").getAsString(),
						dbconn.get("dbName").getAsString(), dbconn.get("user").getAsString(),
						dbconn.get("pwd").getAsString(), dbconn.get("conector").getAsString(),
						dbconn.get("query").getAsString()));
			}
		}
	}
	
	public static <T> void main(String[] args) throws IOException {
		// Read Json File and establishes Lists of DBConnectors for Execution
		establishConnectors(args[0]);
		
		ExecutorService executeOthers = Executors.newFixedThreadPool(otherConectores.size());
		ExecutorService executeMSSQL = Executors.newCachedThreadPool();
		if (!mssqlconectores.isEmpty()) {
			executeMSSQL = Executors.newFixedThreadPool(mssqlconectores.size());
		}
//		List <String> outs = new ArrayList<>();
//		boolean needRollback = false;
		try {
//			List<Future<Void>> afterexec = executeOthers.invokeAll(otherConectores);
//			List<Future<Void>> afterexecMSSQL = new ArrayList<>();
			
			executeOthers.invokeAll(otherConectores);
			
			if (!mssqlconectores.isEmpty()) {
//				afterexecMSSQL = executeMSSQL.invokeAll(mssqlconectores);
				executeMSSQL.invokeAll(mssqlconectores);
			}
			
//			execute.awaitTermination(1, TimeUnit.MINUTES);
			executeOthers.shutdown();
			executeMSSQL.shutdown();
			
			for (DBConnector dbConn : otherConectores) {
				JsonObject state = dbConn.closeConnections();
				System.out.println(new Date().toString() + " " + state.get("idConnector") + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
			}
			for (DBConnector dbConn : mssqlconectores) {
				JsonObject state = dbConn.closeConnections();
				System.out.println(new Date().toString() + " " + state.get("idConnector") + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
			}
			
			if(!DBConnector.isAllGood()) {
				//Log Auditory
				DBConnector logConnector = new DBConnector("Auditoria", logAudParams.get("host").getAsString(),
						logAudParams.get("port").getAsString(), logAudParams.get("serverName").getAsString(),
						logAudParams.get("dbName").getAsString(), logAudParams.get("user").getAsString(),
						logAudParams.get("pwd").getAsString(), logAudParams.get("conector").getAsString(),
						logAudParams.get("query").getAsString());
				logConnector.makeRequest();
				JsonObject state = logConnector.closeConnections();
				System.out.println(new Date().toString() + " " + state.get("idConnector") + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}