import java.sql.*;
import java.util.*;

public class ConnectionManager {
    private static Stack<Connection> connections = new Stack<Connection>();
    
    static {
    	try { Class.forName("org.sqlite.JDBC"); }
    	catch (Exception e) {System.out.println(e);}; 		
    }
    
    public static Connection getConnection() throws Exception {
        if (!connections.empty())
            return connections.pop();
        else //No one left in the stack, create a new one
        	return DriverManager.getConnection("jdbc:sqlite:reuters.db");
    }
    
    public static void returnConnection(Connection conn) {
        if (conn != null) connections.push(conn);
    }      
}