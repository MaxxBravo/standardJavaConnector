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
	
	static boolean allGood = true;
	public String conector, query, rollback;
	public String idConnector, user, pwd, host, port, serverName, dbName;
	
	//Constructor General
	public DBConnector(String idConnector, String host, String port, String serverName, String dbName, String user, String pwd,
			String conector, String query, String rollback) {
		this.idConnector = idConnector;
		this.host = host;
		this.port = port;
		this.serverName = serverName;
		this.dbName = dbName;

		this.user = user;
		this.pwd = pwd;
		this.conector = conector;
		
		this.query = query;
		this.rollback = rollback;
	}


	public void makeRequest() throws IOException, SQLException{
			
		try {
			System.out.println(new Date().toString()+ " Starting connector \"" + this.idConnector + "\" assignment.");
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
				conn = DriverManager.getConnection(url);
				
			} else {
				throw new WrongDatabaseException();
			}
			
			String sql = this.query;
			conn.setAutoCommit(false);
			st = conn.createStatement();
			
			st.executeUpdate(sql);
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
			if(allGood) {
				if (st != null) {
					st.close();
				}
				if (conn != null) {
					conn.commit();
					conn.close();
				}
			} else {
				if (st != null) {
					st.close();
				}
				if (conn != null) {
					conn.rollback();
					conn.close();
				}
			}
		System.out.println(new Date().toString()+ " Run last part of \"" + this.idConnector + "\".");
		return state;
	}
	
	@Override
	public Void call() throws Exception {
		//return this.host + ":" + this.port;
		this.makeRequest();
		return null;
	}
}
