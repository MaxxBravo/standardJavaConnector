package standardJavaConnector;
/* https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html
 * 
 * */

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Handler {

	public static List<DBConnector> otherConectores = new ArrayList<>();
	public static List<DBConnector> mssqlconectores = new ArrayList<>();

	public static void cleanConnectors(List<String> outs) {
		for (String out_name : outs) {
			for (int i = 0; i < otherConectores.size(); i++) {
				if (otherConectores.get(i).idConnector.equals(out_name)) {
					otherConectores.remove(i);
				} else {
					otherConectores.get(i).query = otherConectores.get(i).rollback;
				}
			}
		}
	}

	public static <T> void main(String[] args) throws IOException {
		String json_path = args[0];

		JsonParser parser = new JsonParser();

		JsonObject elem = parser.parse(new FileReader(json_path)).getAsJsonObject();

		for (Entry<String, JsonElement> obj : elem.entrySet()) {
//			System.out.println(obj.getKey());
			JsonObject dbconn = obj.getValue().getAsJsonObject();

//			System.out.println(dbconn.get("host"));
			if (dbconn.get("conector").getAsString().equals("MSSQL")) {
				mssqlconectores.add(new DBConnector(obj.getKey(), dbconn.get("host").getAsString(),
						dbconn.get("port").getAsString(), dbconn.get("serverName").getAsString(),
						dbconn.get("dbName").getAsString(), dbconn.get("user").getAsString(),
						dbconn.get("pwd").getAsString(), dbconn.get("conector").getAsString(),
						dbconn.get("query").getAsString(), dbconn.get("rollback").getAsString()));
			} else {
				otherConectores.add(new DBConnector(obj.getKey(), dbconn.get("host").getAsString(),
						dbconn.get("port").getAsString(), dbconn.get("serverName").getAsString(),
						dbconn.get("dbName").getAsString(), dbconn.get("user").getAsString(),
						dbconn.get("pwd").getAsString(), dbconn.get("conector").getAsString(),
						dbconn.get("query").getAsString(), dbconn.get("rollback").getAsString()));
			}

//			break;
		}
//		System.out.println(conectores.get(1).host);
		ExecutorService execute = Executors.newFixedThreadPool(otherConectores.size());
		ExecutorService executeMSSQL = Executors.newCachedThreadPool();
		if (!mssqlconectores.isEmpty()) {
			executeMSSQL = Executors.newFixedThreadPool(mssqlconectores.size());
		}
//		List <String> outs = new ArrayList<>();
//		boolean needRollback = false;
		try {
			List<Future<JsonObject>> afterexec = execute.invokeAll(otherConectores);
			List<Future<JsonObject>> afterexecMSSQL = new ArrayList<>();
			if (!mssqlconectores.isEmpty()) {
				afterexecMSSQL = executeMSSQL.invokeAll(mssqlconectores);
			}
			
//			execute.awaitTermination(1, TimeUnit.MINUTES);
			execute.shutdown();
			executeMSSQL.shutdown();
			for (Future<JsonObject> fut : afterexec) {
				System.out.println(new Date().toString() + " " + fut.get().get("idConnector") + ": "
						+ fut.get().get("execution") + " - " + fut.get().get("error_message"));
			}

//			executeMSSQL.awaitTermination(1, TimeUnit.MINUTES);

			if (!mssqlconectores.isEmpty()) {
				executeMSSQL.shutdown();
				for (Future<JsonObject> fut : afterexecMSSQL) {
					System.out.println(new Date().toString() + " " + fut.get().get("idConnector") + ": "
							+ fut.get().get("execution") + " - " + fut.get().get("error_message"));
				}
			}
//				if(fut.get().get("execution").getAsString().equals("error")) {
//				outs.add(fut.get().get("idConnector").getAsString());
//				needRollback = true;
//			}		

//			Future<JsonObject> fut = execute.submit(conectores.get(0));
//			System.out.println(fut.get().get("idConnector") + ": " + fut.get().get("execution") + " - " + fut.get().get("error_message"));
//			fut = execute.submit(conectores.get(1));
//			System.out.println(fut.get().get("idConnector") + ": " + fut.get().get("execution") + " - " + fut.get().get("error_message"));

//			ROLLBACK
//			if(needRollback) {
//				System.out.println("Executing roolback...");
////				cleanConnectors(outs);
////				
////				afterexec = execute.invokeAll(conectores);
////				execute.shutdown();
//				System.out.println("Rollback Done.");
//			} 
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

	}
}