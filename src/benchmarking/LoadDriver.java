package benchmarking;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LoadDriver implements Runnable {
	protected Connection conn;
	public int numberOfTransactions;
	private PreparedStatement kontostandStatement;
	private PreparedStatement analyseStatement;
	
	private PreparedStatement procedureEinzahlung;
	private PreparedStatement getProcedureResultSet;
	private BenchmarkCapable systemUnderTest;
	
	public LoadDriver(String connectionString, String user, String password) throws Exception {
		systemUnderTest = BenchmarkController.getInstance("mysql").systemUnderTest;
		conn = systemUnderTest.getConnection(connectionString, user, password);
		//conn = BenchmarkController.getInstance("mysql").systemUnderTest.getConnection(connectionString, user, password);
		numberOfTransactions = 0;
		kontostandStatement = conn.prepareStatement("SELECT balance FROM accounts WHERE accid = ?");
		analyseStatement = conn.prepareStatement("select count(*) from history where delta = ?");
		
		procedureEinzahlung = conn.prepareStatement("call einzahlung(?, ?, ?, ?, ?, @newbalance);");
		getProcedureResultSet = conn.prepareStatement("select @newbalance LIMIT 0, 1000;");
	}

	public void run() {
		int accounts = 1000000;
		int branches = 10;
		int tellers = 100;

		try{
			accounts = systemUnderTest.getNumberOfAccounts(conn);
			branches = systemUnderTest.getNumberOfBranches(conn);
			tellers = systemUnderTest.getNumberOfTellers(conn);
		} catch (SQLException e) {
			
		}

		double random;

		try {
			for (; true; ++numberOfTransactions) {
				Thread.sleep(50);

				random = Math.random();
				if (random >= 0.35 && random < 0.85) {
					systemUnderTest.payment(procedureEinzahlung, getProcedureResultSet, (int) (Math.random() * accounts), (int) (Math.random() * tellers), (int) (Math.random() * branches), (int) (Math.random() * 12345 + 1));
				} else if (random < 0.35) {
					systemUnderTest.getAccountBalance(kontostandStatement, (int) (Math.random() * accounts));
				} else {
					systemUnderTest.getNumberOfDeltas(analyseStatement,(int) (Math.random() * 10000 + 1));
				}
			}
		} catch (InterruptedException e) {
			return;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("\n\nBeendet durch Fehler\n\n");
	}
}
