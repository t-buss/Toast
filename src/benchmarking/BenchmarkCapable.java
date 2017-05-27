package benchmarking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface BenchmarkCapable {

	/**
	 * Connect to a database Gets a connection to a SQL-Database
	 * 
	 * @param networkpath
	 *            The path in the form "ip:port/database"
	 * @param username
	 *            Used to authentificate the connection
	 * @param password
	 *            Used to authentificate the connection
	 * @return The connection established as a java.sql.Connection object, null
	 *         if the connection could not be established
	 */
	Connection getConnection(String networkpath, String username, String password);

	/**
	 * Creates the schemas for the tables BRANCHES, ACCOUNTS and TELLERS in the
	 * connected database
	 * @throws SQLException 
	 */
	void initDbSchema(Connection conn) throws SQLException;

	/**
	 * Deletes the tables BRANCHES, ACCOUNTS and TELLERS from the database
	 * @throws SQLException 
	 */
	void dropTables(Connection conn) throws SQLException;

	int getAccountBalance(PreparedStatement kontostandStatement, int accid) throws SQLException;

	int getNumberOfDeltas(PreparedStatement kontostandStatement, int delta) throws SQLException;

	int payment(PreparedStatement procedureEinzahlung, PreparedStatement getProcedureResultSet, int accid, int tellerid, int branchid, int delta) throws SQLException;

	void fillBranches(Connection conn, int numberOfBranches) throws SQLException;

	void fillAccounts(Connection conn, int numberOfAccounts, int numberOfBranches) throws SQLException;

	void fillTellers(Connection conn, int numberOfTellers, int numberOfBranches) throws SQLException;

	void truncateAll(Connection conn) throws SQLException;

	void truncateBranches(Connection conn) throws SQLException;

	void truncateAccounts(Connection conn) throws SQLException;

	void truncateTellers(Connection conn) throws SQLException;

	void truncateHistory(Connection conn) throws SQLException;

	int getNumberOfBranches(Connection conn) throws SQLException;

	int getNumberOfAccounts(Connection conn) throws SQLException;

	int getNumberOfTellers(Connection conn) throws SQLException;

	int getNumberOfHistoryEntries(Connection conn) throws SQLException;
}
