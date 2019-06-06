package standardJavaConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import ExceptionsCustom.WrongDatabaseException;

public class LogConnector implements Callable<Void>{
	JsonObject state = new JsonObject();
	JsonArray result_description = new JsonArray();
	Map<String, List<String>> logEjecuciones = new HashMap<>();	
	
	Connection conn = null;
	Statement st = null;
		
	private static boolean allGood = true;
	private static boolean debugModeCon = false;
	
	public String conector;
	public JsonArray query;
	public String idConnector, user, pwd, host, port, serverName, dbName;

	//Constructor General
	public LogConnector(String idConnector, String host, String port, String serverName, String dbName, String user, String pwd,
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


	public void setLogEjecuciones(Map<String, List<String>> logEjecuciones) {
		this.logEjecuciones = logEjecuciones;
	}

	public static boolean isAllGood() {
		return allGood;
	}
	
	public static void setDebugModeCon(boolean debugModeCon) {
		LogConnector.debugModeCon = debugModeCon;
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
				JsonObject queryObject = elem.getAsJsonObject();

				if(queryObject.get("type").getAsString().equalsIgnoreCase("UPDATE")){
					
					//Armar query desde los datos dispersados del JSON request desde el servidor
					JsonObject script = queryObject.getAsJsonObject("script");
					String queryToExecute = script.get("cabecera").getAsString() + " " + script.get("data").getAsString();
					
					if(logEjecuciones.get(queryObject.get("_idBase").getAsString()).contains(queryObject.get("_idScriptToLog").getAsString())) {
						String applied = " 'Y' ";
						queryToExecute += applied;
						
					} else {
						String notApplied = " 'N' ";
						queryToExecute += notApplied;
						
					}
					queryToExecute += script.get("end").getAsString();
					
					st.addBatch(queryToExecute);
					
				} else if(queryObject.get("type").getAsString().equalsIgnoreCase("INSERT")) {
					if(logEjecuciones.get(queryObject.get("_idBase").getAsString()).contains(queryObject.get("_idScriptToLog").getAsString())) {
						st.addBatch(queryObject.get("script").getAsString());	
					}
					
				} else if(queryObject.get("type").getAsString().equalsIgnoreCase("JUMPSTEP")) {
					st.addBatch(queryObject.get("script").getAsString());
				}
			}

			int [] arr_int = st.executeBatch();

			for (int i : arr_int) {
				JsonObject _to_add = new JsonObject();
				_to_add.addProperty(String.valueOf(i), i);
				result_description.add(_to_add.remove(String.valueOf(i)));
			}
			
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
