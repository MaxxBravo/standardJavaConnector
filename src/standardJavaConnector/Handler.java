package standardJavaConnector;
/* https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html
 * 
 * */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
	public static DBConnector logConnector;
	static JsonObject responseObject = new JsonObject(); 
	static JsonObject logAudParams = new JsonObject();
	static boolean debugMode = false;
	
	public static void establishConnectors(String json_path) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		JsonParser parser = new JsonParser();

		JsonObject down_elem = parser.parse(new InputStreamReader(new FileInputStream(json_path), StandardCharsets.UTF_8)).getAsJsonObject();
		
		JsonObject elem = down_elem.get("changes").getAsJsonObject();
		logAudParams = down_elem.get("auditory").getAsJsonObject();
			
		logConnector = new DBConnector("Auditoria", 
				logAudParams.has("host") ? logAudParams.get("host").getAsString() : "", 
				logAudParams.has("port") ? logAudParams.get("port").getAsString() : "", 
				logAudParams.has("serverName") ? logAudParams.get("serverName").getAsString() : "", 
				logAudParams.has("dbName") ? logAudParams.get("dbName").getAsString() : "", 
				logAudParams.has("user") ? logAudParams.get("user").getAsString() : "", 
				logAudParams.has("pwd") ? logAudParams.get("pwd").getAsString() : "", 
				logAudParams.has("conector") ? logAudParams.get("conector").getAsString() : "", 
				logAudParams.get("query").getAsJsonArray());
		
		
		for (Entry<String, JsonElement> obj : elem.entrySet()) {
			JsonObject dbconn = obj.getValue().getAsJsonObject();
			
			String host = dbconn.has("host") ? dbconn.get("host").getAsString() : "";	
			String port = dbconn.has("port") ? dbconn.get("port").getAsString() : "";
			String serverName = dbconn.has("serverName") ? dbconn.get("serverName").getAsString() : "";
			String dbName = dbconn.has("dbName") ? dbconn.get("dbName").getAsString() : "";
			String user = dbconn.has("user") ? dbconn.get("user").getAsString() : "";
			String pwd = dbconn.has("pwd") ? dbconn.get("pwd").getAsString() : "";
			String conector = dbconn.has("conector") ? dbconn.get("conector").getAsString() : "";
		
			if (conector.equals("MSSQL")) {
				mssqlconectores.add(new DBConnector(obj.getKey(), host,	port, serverName,
						dbName, user, pwd, conector, dbconn.get("query").getAsJsonArray()));
			} else {
				otherConectores.add(new DBConnector(obj.getKey(), host,	port, serverName,
						dbName, user, pwd, conector, dbconn.get("query").getAsJsonArray()));
			}
		}
	}

	
	public static void main(String[] args) throws IOException {
		// Read Json file and establishes Lists of DBConnectors for Execution
		establishConnectors(args[0]);
		// Set debugMode
		String arg1 = "";
		try {
			arg1 = args[1];
		} catch (ArrayIndexOutOfBoundsException e) {
			arg1 = "false";
		}
		debugMode = Boolean.parseBoolean(arg1);
		DBConnector.setDebugModeCon(debugMode);
		
		
		ExecutorService executeOthers = Executors.newCachedThreadPool();
		if(!otherConectores.isEmpty()) {
			executeOthers = Executors.newFixedThreadPool(otherConectores.size());
		}
		
		ExecutorService executeMSSQL = Executors.newCachedThreadPool();
		if (!mssqlconectores.isEmpty()) {
			executeMSSQL = Executors.newFixedThreadPool(mssqlconectores.size());
		}

		try {
			if(!otherConectores.isEmpty()) {
				executeOthers.invokeAll(otherConectores);
			}
			if (!mssqlconectores.isEmpty()) {
				executeMSSQL.invokeAll(mssqlconectores);
			}
			executeOthers.shutdown();
			executeMSSQL.shutdown();
			
			for (DBConnector dbConn : otherConectores) {
				JsonObject state = dbConn.closeConnections();
				String idConnector = state.remove("idConnector").getAsString();
				responseObject.add(idConnector, state);
				if(debugMode) {
					System.out.println(new Date().toString() + " " + idConnector + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
				}
			}
			for (DBConnector dbConn : mssqlconectores) {
				JsonObject state = dbConn.closeConnections();
				String idConnector = state.remove("idConnector").getAsString();
				responseObject.add(idConnector, state);
				if(debugMode) {
					System.out.println(new Date().toString() + " " + idConnector + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
				}
			}
			
			if(DBConnector.isAllGood()) {
				//Log Auditory
				
				logConnector.makeRequest();
				JsonObject state = logConnector.closeConnections();
				String idConnector = state.remove("idConnector").getAsString();
				responseObject.add(idConnector, state);
				if(debugMode) {
					System.out.println(new Date().toString() + " " + idConnector + ": "
					+ state.get("execution") + "_" + state.get("resolution") + " - " + state.get("error_message"));
				}
			}
			if(!debugMode) {
				System.out.println(responseObject.toString());
			}			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}