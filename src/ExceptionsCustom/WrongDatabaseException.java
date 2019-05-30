package ExceptionsCustom;

public class WrongDatabaseException extends Exception{

	private static final long serialVersionUID = 1L;

	public WrongDatabaseException() {
		super("Database Connector not available");
	}
	
}
