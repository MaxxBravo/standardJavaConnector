package standardJavaConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import ExceptionsCustom.WrongDatabaseException;


public class DBConnector implements Callable<Void>{
	JsonObject state = new JsonObject();
	JsonArray result_description = new JsonArray();
	Connection conn = null;
	Statement st = null;
	
	private static boolean allGood = true;
	private static boolean debugModeCon = false;
	private static Map<String, List<String>> logEjecuciones = new HashMap<>();
	
	private List <String> listQueriesToRun = new ArrayList<>();
	
	public String conector;
	public JsonArray query;
	public String idConnector, user, pwd, host, port, serverName, dbName;
//	public int rowsAffected;
	
	//Constructor General
	public DBConnector(String idConnector, String host, String port, String serverName, String dbName, String user, String pwd,
			String conector, JsonArray query) {
		
		this.idConnector = idConnector;
		this.host = host;
		this.port = port;
		this.serverName = serverName;
		this.dbName = dbName;

		this.user = user;
		this.pwd = pwd;
		this.conector = conector;
		
		this.query = query;
	}


	public static boolean isAllGood() {
		return allGood;
	}
	
	public static void setDebugModeCon(boolean debugModeCon) {
		DBConnector.debugModeCon = debugModeCon;
	}
	
	public static Map<String, List<String>> getLogEjecuciones() {
		return logEjecuciones;
	}
	
	public void makeRequest() throws IOException, SQLException{
			
		try {
			if(debugModeCon) {
				System.out.println(new Date().toString()+ " Starting connector \"" + this.idConnector + "\" assignment.");
			}
			String url = "";
			
			if (this.conector.equals("NTZ")) {
				url = "jdbc:netezza://" + this.host + ":" + this.port + "/" + this.dbName;
				Class.forName("org.netezza.Driver");
				conn = DriverManager.getConnection(url, this.user, this.pwd);

			} else if (this.conector.equals("MSSQL")) {
				url = "jdbc:sqlserver://"
						+ this.host+":"+this.port+";"
						+ "integratedSecurity=true;";		
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				conn = DriverManager.getConnection(url);

			} else if (this.conector.equals("SYB")) {
				url = "jdbc:sqlanywhere:"
						+ "Host="+this.host+":"+this.port+";"
						+ "ServerName="+this.serverName+";"
						+ "DBN="+this.dbName+";"
						+ "KRB=SSPI";
				Class.forName("sybase.jdbc4.sqlanywhere.IDriver");
				conn = DriverManager.getConnection(url);
				
			} else {
				throw new WrongDatabaseException();
			}
			
			conn.setAutoCommit(false);
			st = conn.createStatement();
			JsonArray queries = this.query;
			
			for(JsonElement elem : queries) {
				JsonObject query_object = elem.getAsJsonObject();
				listQueriesToRun.add(query_object.get("_idScript").getAsString());
				
				
				st.addBatch(query_object.get("script").getAsString());
			}
			
			int [] arr_int = st.executeBatch();

//			for (int i : arr_int) {
//				listQueriesToRun.set(index, element)
//				JsonObject _to_add = new JsonObject();
//				_to_add.addProperty(String.valueOf(i), i);
//				result_description.add(_to_add.remove(String.valueOf(i)));
//			}
			
			for(int index = 0; index<arr_int.length; index++) {
				if(arr_int[index] == 0) {
					listQueriesToRun.set(index, "none");
				}
				
				JsonObject _to_add = new JsonObject();
				_to_add.addProperty(String.valueOf(arr_int[index]), arr_int[index]);
				result_description.add(_to_add.remove(String.valueOf(arr_int[index])));
			}
			
			while(listQueriesToRun.remove("none")){}
			
			logEjecuciones.put(this.idConnector, listQueriesToRun);
				
			
			state.addProperty("idConnector", this.idConnector);
			state.addProperty("execution", "ok");
			state.addProperty("conector", this.conector);
			
		} catch (Exception e) {
			
			allGood = false;
			state.addProperty("idConnector", this.idConnector);
			state.addProperty("conector", this.conector);
			state.addProperty("execution", "error");
			state.addProperty("error_message", e.getLocalizedMessage());
		}
	}
	
	public JsonObject closeConnections() throws SQLException {
		if (allGood) {
			if (st != null) {
				st.close();
			}
			if (conn != null) {
				conn.commit();
				conn.close();
			}

			state.add("afected_rows", result_description);
			state.addProperty("resolution", "commited");
		} else {
			if (st != null) {
				st.close();
			}
			if (conn != null) {
				conn.rollback();
				conn.close();
			}
			state.addProperty("resolution", "rollbacked");
		}
		if(debugModeCon) {
			System.out.println(new Date().toString() + " Run last part of \"" + this.idConnector + "\".");
		}
		return this.state;
	}

	@Override
	public Void call() throws Exception {
		this.makeRequest();
		return null;
	}


	
}
