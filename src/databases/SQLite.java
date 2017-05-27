package databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import benchmarking.BenchmarkCapable;

public class SQLite implements BenchmarkCapable {

	@Override
	public Connection getConnection(String networkpath, String username, String password) {
		Connection conn = null;
		try {
			Class.forName("org.sqlite.JDBC").newInstance();
			DriverManager.setLoginTimeout(20);
			conn = DriverManager.getConnection("jdbc:sqlite:" + networkpath);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		} catch (SQLException e) {
			System.out.println("Fehlgeschlagen:");
			e.printStackTrace();
		} catch (InstantiationException e) {
			System.out.println("Fehlgeschlagen:");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			System.out.println("Fehlgeschlagen:");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("Fehlgeschlagen:");
			System.out.println("Treiber-Klasse wurde nicht gefunden: org.sqlite.JDBC\nDie Klasse muss dem Build-Path hinzugefügt werden");
		}
		return conn;
	}

	@Override
	public void initDbSchema(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();

		stmt.executeUpdate("create table if not exists branches(branchid int not null," + "branchname char(20) not null,balance int not null,address char(72) " + "not null,primary key (branchid));");

		stmt.executeUpdate("create table if not exists accounts(accid int not null," + "name char(20) not null,balance int not null,branchid int not null,"
				+ "address char(68) not null,primary key (accid),foreign key (branchid) references branches(branchid) );");

		stmt.executeUpdate("create table if not exists tellers(tellerid int not null," + "tellername char(20) not null,balance int not null,branchid int not null,"
				+ "address char(68) not null,primary key (tellerid),foreign key (branchid) references branches(branchid) );");

		stmt.executeUpdate("create table if not exists history (accid int not null,tellerid int not null, " + "delta int not null,branchid int not null,accbalance int not null, "
				+ "cmmnt char(30) not null,foreign key (accid) references accounts(accid), "
				+ "foreign key (tellerid) references tellers(tellerid), foreign key (branchid) references branches(branchid) );");

	}

	@Override
	public void dropTables(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table history;");
		stmt.executeUpdate("drop table tellers;");
		stmt.executeUpdate("drop table accounts;");
		stmt.executeUpdate("drop table branches;");
	}

	@Override
	public int getAccountBalance(PreparedStatement kontostandStatement, int accid) throws SQLException {
		kontostandStatement.setInt(1, accid);
		ResultSet rs = kontostandStatement.executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	@Override
	public int getNumberOfDeltas(PreparedStatement kontostandStatement, int delta) throws SQLException {
		kontostandStatement.setInt(1, delta);
		ResultSet rs = kontostandStatement.executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	@Override
	public int payment(PreparedStatement procedureEinzahlung, PreparedStatement getProcedureResultSet, int accid, int tellerid, int branchid, int delta) throws SQLException {
		procedureEinzahlung.setInt(1, accid);
		procedureEinzahlung.setInt(2, tellerid);
		procedureEinzahlung.setInt(3, branchid);
		procedureEinzahlung.setInt(4, delta);
		procedureEinzahlung.setString(5, "Toastbrot ist echt voll toefte");
		procedureEinzahlung.execute();
		ResultSet rs2 = getProcedureResultSet.executeQuery();
		rs2.next();
		return rs2.getInt(1);
	}

	@Override
	public void fillBranches(Connection conn, int numberOfBranches) throws SQLException {
		Statement stmt = conn.createStatement();
		StringBuilder sb = new StringBuilder();
		sb.append("insert into branches (branchname, balance, address, branchid) values ('AX7cNy9z4uYkzHCS6ljC',0,'b16oReZoZDqOc8kumDwjtbfZI7f1Ovd7Vyz83GQNALxW5IItU6IVaAZscCEQRVRA36D7mlT0',0)");

		for (int i = 1; i < numberOfBranches; ++i) {

			sb.append(",('AX7cNy9z4uYkzHCS6ljC',0,'b16oReZoZDqOc8kumDwjtbfZI7f1Ovd7Vyz83GQNALxW5IItU6IVaAZscCEQRVRA36D7mlT0',").append(i).append(")");

			if (sb.length() >= 99500) {
				sb.append(";");
				stmt.addBatch(sb.toString());
				sb.delete(0, sb.length());

				sb.append("insert into branches (branchname, balance, address, branchid) values ('AX7cNy9z4uYkzHCS6ljC',0,'b16oReZoZDqOc8kumDwjtbfZI7f1Ovd7Vyz83GQNALxW5IItU6IVaAZscCEQRVRA36D7mlT0',")
						.append(++i).append(")");

			}
		}
		stmt.addBatch(sb.toString());
		stmt.executeBatch();
	}

	@Override
	public void fillAccounts(Connection conn, int numberOfAccounts, int numberOfBranches) throws SQLException {
		Statement stmt = conn.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("insert into accounts (name, balance, branchid, address, accid) values ('4nXafmMupp7we06EwP6O',0,").append((int) (Math.random() * numberOfBranches))
				.append(",'CbIa5jNQnTQ1kWlzrhi5EUOBZbIftwfenjrTVFtKbqCxLQSy1uwp0Ts465havFA4evZA',0)");

		for (int i = 1; i < numberOfAccounts; ++i) {

			sb.append(",('4nXafmMupp7we06EwP6O',0,").append((int) (Math.random() * numberOfBranches)).append(",'CbIa5jNQnTQ1kWlzrhi5EUOBZbIftwfenjrTVFtKbqCxLQSy1uwp0Ts465havFA4evZA',").append(i)
					.append(")");

			if (sb.length() >= 9500) {
				sb.append(";");
				stmt.executeUpdate(sb.toString());
				sb.delete(0, sb.length());
				sb.append("insert into accounts (name, balance, branchid, address, accid) values ('4nXafmMupp7we06EwP6O',0,").append(((int) Math.random() * numberOfBranches)).append(",'',")
						.append(++i).append(")");
			}
		}
		stmt.executeUpdate(sb.toString());
	}

	@Override
	public void fillTellers(Connection conn, int numberOfTellers, int numberOfBranches) throws SQLException {
		Statement stmt = conn.createStatement();
		StringBuilder sb = new StringBuilder();
		sb.append("insert into tellers (tellername, balance, branchid, address, tellerid) values ('aITqdVHiB1bEOCZ74bA7',0,").append((int) (Math.random() * numberOfBranches))
				.append(",'K2F4oxWX0SvsXAbrTZxj0Eb1mbxMa7r9vlgulPymeJRj1KZ463EubYUGDYfau6jUIfR5',0)");

		for (int i = 1; i < numberOfTellers; ++i) {

			sb.append(",('aITqdVHiB1bEOCZ74bA7',0,").append((int) (Math.random() * numberOfBranches)).append(",'K2F4oxWX0SvsXAbrTZxj0Eb1mbxMa7r9vlgulPymeJRj1KZ463EubYUGDYfau6jUIfR5',").append(i)
					.append(")\n");

			if (sb.length() >= 99500) {
				sb.append(";");
				stmt.addBatch(sb.toString());
				sb.delete(0, sb.length());

				sb.append("insert into tellers (tellername, balance, branchid, address, tellerid) values ('aITqdVHiB1bEOCZ74bA7',0,").append(((int) Math.random() * numberOfBranches))
						.append(",'K2F4oxWX0SvsXAbrTZxj0Eb1mbxMa7r9vlgulPymeJRj1KZ463EubYUGDYfau6jUIfR5',").append(++i).append(")");
			}
		}
		stmt.addBatch(sb.toString());
		stmt.executeBatch();
	}

	@Override
	public void truncateAll(Connection conn) throws SQLException {
		Statement stmt;
		stmt = conn.createStatement();
		stmt.executeUpdate("delete from history");
		stmt.executeUpdate("delete from tellers");
		stmt.executeUpdate("delete from accounts");
		stmt.executeUpdate("delete from branches");
	}

	@Override
	public void truncateBranches(Connection conn) throws SQLException {
		Statement stmt;
		stmt = conn.createStatement();
		stmt.executeUpdate("delete from branches");
	}

	@Override
	public void truncateAccounts(Connection conn) throws SQLException {
		Statement stmt;
		stmt = conn.createStatement();
		stmt.executeUpdate("delete from accounts");
	}

	@Override
	public void truncateTellers(Connection conn) throws SQLException {
		Statement stmt;
		stmt = conn.createStatement();
		stmt.executeUpdate("delete from tellers");
	}

	@Override
	public void truncateHistory(Connection conn) throws SQLException {
		Statement stmt;
		stmt = conn.createStatement();
		stmt.executeUpdate("delete from history");
	}

	@Override
	public int getNumberOfBranches(Connection conn) throws SQLException {
		ResultSet rs = conn.prepareStatement("SELECT COUNT(*) FROM branches").executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	@Override
	public int getNumberOfAccounts(Connection conn) throws SQLException {
		ResultSet rs = conn.prepareStatement("SELECT COUNT(*) FROM accounts").executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	@Override
	public int getNumberOfTellers(Connection conn) throws SQLException {
		ResultSet rs = conn.prepareStatement("SELECT COUNT(*) FROM tellers").executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	@Override
	public int getNumberOfHistoryEntries(Connection conn) throws SQLException {
		ResultSet rs = conn.prepareStatement("SELECT COUNT(*) FROM history").executeQuery();
		rs.next();
		return rs.getInt(1);
	}

}
