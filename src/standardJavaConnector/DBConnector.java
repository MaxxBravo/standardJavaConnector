package standardJavaConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.Callable;

import com.google.gson.JsonObject;

class WrongDatabaseException extends Exception {

	private static final long serialVersionUID = 1L;

	public WrongDatabaseException() {
		super("Database Connector not available");
    }
}

public class DBConnector implements Callable<Void>{
	JsonObject state = new JsonObject();
	Connection conn = null;
	Statement st = null;
	
	private static boolean allGood = true;
	private static boolean debugModeCon = false;
	
	public String conector, query;
	public String idConnector, user, pwd, host, port, serverName, dbName;
	public int rowsAffected;
	
	//Constructor General
	public DBConnector(String idConnector, String host, String port, String serverName, String dbName, String user, String pwd,
			String conector, String query) {
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
			
			String sql = this.query;
			conn.setAutoCommit(false);
			st = conn.createStatement();
			
			rowsAffected = st.executeUpdate(sql);
			state.addProperty("idConnector", this.idConnector);
			state.addProperty("execution", "ok");

		} catch (Exception e) {
			allGood = false;
			state.addProperty("idConnector", this.idConnector);
			state.addProperty("execution", "error");
			state.addProperty("error_message", e.getLocalizedMessage());
		}
	}
	
	public JsonObject makeRequest2() {
		JsonObject obj = new JsonObject();
		obj.addProperty("idConnector", this.idConnector);
		obj.addProperty("execution", this.host);
		obj.addProperty("error_message", this.port);
		return obj;
		
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
			state.addProperty("afected_rows", rowsAffected);
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
