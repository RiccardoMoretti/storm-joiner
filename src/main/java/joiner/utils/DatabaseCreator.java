package joiner.utils;

/* A connection (session) with a specific database. SQL statements 
 *  are executed and results are returned within the context of a connection.
 * A Connection object's database is able to provide information 
 * 	describing its tables, its supported SQL grammar, its stored procedures,
 * 	the capabilities of this connection, and so on. 
 * This information is obtained with the getMetaData method.
*/
import java.sql.Connection;

/* An object that represents a precompiled SQL statement. 
 * A SQL statement is precompiled and stored in a PreparedStatement object. 
 * This object can then be used to efficiently execute this statement multiple times.
*/
import java.sql.PreparedStatement;

/* The object used for executing a static SQL statement and returning 
 * 	the results it produces. 
 * By default, only one ResultSet object per Statement object can be open 
 * 	at the same time. Therefore, if the reading of one ResultSet object 
 *	is interleaved with the reading of another, each must have been generated 
 * 	by different Statement objects. 
 * All execution methods in the Statement interface implicitly close a s
 * tatment's current ResultSet object if an open one exists.
*/
import java.sql.Statement;


public abstract class DatabaseCreator {
	
	protected static void create(Connection connection, String table, String column, int from, int to, int step, int repetitions) throws Exception {
		
		Statement stmt = connection.createStatement();
		
	// create table "table" with index "main" ( not null ) 	
		stmt.execute(String.format("CREATE TABLE %s (%s int NOT NULL)", table, column));
		stmt.execute(String.format("CREATE INDEX main ON %s (%s)", table, column));
	
	// make a ps for insert value into "table"
		PreparedStatement ps = connection.prepareStatement(String.format("INSERT INTO %s VALUES (?)", table));
	
		
	for (int i = from; i <= to; i += step) {
			
	//Sets the designated parameter to the given Java int value.
			ps.setInt(1, i);
			
			for (int j = 0; j < repetitions; ++j)
	
	//Adds a set of parameters to this PreparedStatement object's batch of commands.
				ps.addBatch();
		}
	
	//Submits a batch of commands to the database for execution 	
		ps.executeBatch();
		
		connection.commit();
		connection.close();
	}

}
