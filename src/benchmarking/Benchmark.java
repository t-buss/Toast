package benchmarking;
/**
 * Hauptklasse für das Benchmark-Programm, sowohl nach Aufgabe 1 und Aufgabe 2
 * Die Benchmarks lassen sich über ein Menü auswählen, zusammen mit einigen nützlichen Funktionen für die Wartung der Datenbank
 * @author Buß, Thomas <br />Temminhoff, Jan <br />Wißing, Stephan
 */

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import consolemenu.*;

public class Benchmark {

	static int numberOfBranches;
	static int numberOfAccounts;
	static int numberOfTellers;
	static Connection conn = null;
	private static String ip = null, database = null, username = null, password = null;

	public static void main(String[] args) throws SQLException, IOException {

		System.out.println("Benchmark-Programm v1.2.\n");
		final Scanner sc = new Scanner(System.in);
		
		if (args.length >= 4) {
			// Verbindungsversuch mit Programm-Parametern
			try {
				System.out.print("Verbinde: ");
				conn = getConnection("jdbc:mysql://" + args[0] + "/" + args[1], args[2], args[3]);
				System.out.println("Fertig!\n");
				ip = args[0];
				database = args[1];
				username = args[2];
				password = args[3];
			} catch (Exception e) {
				System.out.println("Konnte keine Verbindung mit den Parametern herstellen\n");
			}
		}
		
		// Menüstruktur aufbauen (komfort)

		MenuFolder mainmenu = new MenuFolder("Hauptmenü");

		mainmenu.add(new MenuOption("Verbindung aufbauen") {
			@Override
			public void perform() {
				// Verbindung aufbauen
				try {
					getConnectionInfo(sc);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
					System.out.println("Verbindung fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
					conn = null;
				}
			}
		});

		mainmenu.add(new MenuOption("Datenbank-Schema initiieren") {
			@Override
			public void perform() {
				// Datenbank-Schema initiieren
				try {
					System.out.print("Initiere Datenbank: ");
					initDb();
					System.out.println("Fertig!\n");
				} catch (Exception e) {
					System.out.println("Initiierung fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		MenuFolder deleteOptions = new MenuFolder("Löschen");

		mainmenu.add(deleteOptions);

		deleteOptions.add(new MenuOption("Gesamte Datenbank zurücksetzen") {
			@Override
			public void perform() {
				try {
					System.out.print("Setze Datenbank zurueck: ");
					reInitDb();
					System.out.println("Fertig!\n");
				} catch (Exception e) {
					System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		deleteOptions.add(new MenuOption("Alle Tabellen leeren") {
			@Override
			public void perform() {
				Statement stmt;
				try {
					stmt = conn.createStatement();
					stmt.execute("set foreign_key_checks = 0;");
					System.out.print("Loesche alles... ");
					stmt.executeUpdate("truncate history");
					stmt.executeUpdate("truncate tellers");
					stmt.executeUpdate("truncate accounts");
					stmt.executeUpdate("truncate branches");
					stmt.execute("set foreign_key_checks = 1;");
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		});

		deleteOptions.add(new MenuOption("BRANCHES leeren") {
			@Override
			public void perform() {
				try {
					Statement stmt;
					stmt = conn.createStatement();
					stmt.execute("set foreign_key_checks = 0;");
					System.out.print("Loesche BRANCHES... ");
					stmt.executeUpdate("truncate branches");
					stmt.execute("set foreign_key_checks = 1;");
				} catch (Exception e) {
					System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		deleteOptions.add(new MenuOption("ACCOUNTS leeren") {
			@Override
			public void perform() {
				try {
					Statement stmt;
					stmt = conn.createStatement();
					stmt.execute("set foreign_key_checks = 0;");
					System.out.print("Loesche ACCOUNTS... ");
					stmt.executeUpdate("truncate accounts");
					stmt.execute("set foreign_key_checks = 1;");
				} catch (Exception e) {
					System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		deleteOptions.add(new MenuOption("TELLERS leeren") {
			@Override
			public void perform() {
				try {
					Statement stmt;
					stmt = conn.createStatement();
					stmt.execute("set foreign_key_checks = 0;");
					System.out.print("Loesche TELLERS... ");
					stmt.executeUpdate("truncate tellers");
					stmt.execute("set foreign_key_checks = 1;");
				} catch (Exception e) {
					System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		deleteOptions.add(new MenuOption("HISTORY leeren") {
			@Override
			public void perform() {
				try {
					Statement stmt;
					stmt = conn.createStatement();
					stmt.execute("set foreign_key_checks = 0;");
					System.out.print("Loesche HISTORY... ");
					stmt.executeUpdate("truncate history");
					stmt.execute("set foreign_key_checks = 1;");
				} catch (Exception e) {
					System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		mainmenu.add(new MenuOption("Einfüge-Benchmark ausführen") {
			@Override
			public void perform() {
				// Einfüge-Benchmark durchführen
				try {
					getBenchmarkInfoAndRun(sc);
				} catch (Exception e) {
					System.out.println("Benchmark fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		mainmenu.add(new MenuOption("Multithread-Abfrage-Benchmark durchführen") {
			@Override
			public void perform() {
				// Multithread-Benchmark durchführen
				try {
					System.out.print("Anzahl der LoadDriver-Threads angeben: ");
					int threadcount = sc.nextInt();

					// Aufruf von run() in Klasse "MultiThreadBenchmark"
					new MultiThreadBenchmark().run(ip + database, username, password, threadcount);

					System.out.print("Soll die HISTORY-Tabelle zurückgesetzt werden?(j/n)");
					if (sc.next().equals("j")) {
						try {
							System.out.print("Loesche HISTORY... ");
							conn.createStatement().executeUpdate("truncate history");
							System.out.println("Fertig!\n");
						} catch (Exception e) {
							System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
						}
					} else {
						System.out.println("Tabelle bleibt bestehen\n");
					}

				} catch (Exception e) {
					System.out.println("Benchmark fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
				}
			}
		});

		MenuFolder logOptions = new MenuFolder("Logfile Einstellungen");

		mainmenu.add(logOptions);

		logOptions.add(new MenuOption("Logfile anzeigen") {
			@Override
			public void perform() {
				try {
					Logfile.readLog();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		});

		logOptions.add(new MenuOption("Logfile löschen") {
			@Override
			public void perform() {
				try {
					Logfile.clearLog();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		logOptions.add(new MenuOption("Logging ein/ausschalten (AN)") {
			@Override
			public void perform() {
				Logfile.logging = !Logfile.logging;
				if (Logfile.logging) {
					System.out.println("Logging ist jetzt aktiviert");
					this.name = "Logging ein/ausschalten (AN)";
				} else {
					System.out.println("Logging ist jetzt deaktiviert");
					this.name = "Logging ein/ausschalten (AUS)";
				}
			}
		});

		mainmenu.getUserInput();

		System.out.println("\n<<<END OF  LINE>>>");
	}

	/**
	 * Neuinitialiseren der Datenbak Mithilfe von DROP TABLE Anweisungen werden
	 * die Tabellen in der Datenbak gelöscht. Anschließend wird initDb()
	 * gerufen, um sie wiederherzustellen
	 * 
	 * @throws SQLException
	 * @see initDb()
	 */
	public static void reInitDb() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table history;");
		stmt.executeUpdate("drop table tellers;");
		stmt.executeUpdate("drop table accounts;");
		stmt.executeUpdate("drop table branches;");

		initDb();
	}

	/**
	 * Erstellt das Datenbank-Schema für die Benchmark-Messungen Das
	 * Datenbankschema wird durch CREATE TABLE Anweisungen erstellt
	 * 
	 * @throws SQLException
	 */
	public static void initDb() throws SQLException, NullPointerException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create database if not exists bmdb");

		stmt.executeUpdate("create table if not exists branches(branchid int not null," + "branchname char(20) not null,balance int not null,address char(72) " + "not null,primary key (branchid));");

		stmt.executeUpdate("create table if not exists accounts(accid int not null," + "name char(20) not null,balance int not null,branchid int not null,"
				+ "address char(68) not null,primary key (accid),foreign key (branchid) references branches(branchid) );");

		stmt.executeUpdate("create table if not exists tellers(tellerid int not null," + "tellername char(20) not null,balance int not null,branchid int not null,"
				+ "address char(68) not null,primary key (tellerid),foreign key (branchid) references branches(branchid) );");

		stmt.executeUpdate("create table if not exists history (accid int not null,tellerid int not null, " + "delta int not null,branchid int not null,accbalance int not null, "
				+ "cmmnt char(30) not null,foreign key (accid) references accounts(accid), "
				+ "foreign key (tellerid) references tellers(tellerid), foreign key (branchid) references branches(branchid) );");
	}

	/**
	 * Aufrufmethode für fillBranches(), fillAccounts() und fillTellers() Ruft
	 * den Benchmark auf mit dem Befüllen von den Tabellen "branches",
	 * "accounts" und "tellers" Dabei wird jedoch KEIN Commit gemacht, es stellt
	 * also eine einzige Transaktion dar. Anschließend wollte man also
	 * Connection.commit() aufrufen.
	 * 
	 * @throws SQLException
	 */
	public static void fillDb() throws SQLException {
		System.out.print("BRANCHES anlegen:");
		fillBranches();
		System.out.println("\t\t[DONE]");

		System.out.print("ACCOUNTS anlegen:");
		fillAccounts();
		System.out.println("\t\t[DONE]");

		System.out.print("TELLERS anlegen:");
		fillTellers();
		System.out.println("\t\t[DONE]");
	}

	/**
	 * Verbindung mit der Datenbank aufstellen
	 * 
	 * @param ip
	 *            Die Netzwerkadresse des Datenbank-Servers
	 * @param name
	 *            Der Username für die Datenbank
	 * @param password
	 *            Das zum Username gehörende Passwort
	 * @return Connection-Objekt.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static Connection getConnection(String ip, String name, String password) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection conn = null;
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		DriverManager.setLoginTimeout(20);
		conn = DriverManager.getConnection(ip, name, password);
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		return conn;
	}

	/**
	 * Füllt die Accounts-Tabelle mit Testtupeln Dies ist eine zweite Methode
	 * zum Hinzufügen der Einträge in der Tabelle "accounts" , da man manchmal
	 * besser von vorne anfangen kann. Die Statements werden mit einem einen
	 * String-Builder aneinander gehängt, bis die Zeichenlänge knapp 100'000
	 * erreicht. Dann werden die bisherigen Tupel eingefügt, und der
	 * String-Builder wird neu initiiert. Anschließend wird das Verfahren
	 * fortgesetzt.
	 * 
	 * @throws SQLException
	 */
	private static void fillAccounts() throws SQLException {
		Statement stmt = conn.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("insert into accounts (name, balance, branchid, address, accid) values ('4nXafmMupp7we06EwP6O',0,").append((int) (Math.random() * numberOfBranches))
				.append(",'CbIa5jNQnTQ1kWlzrhi5EUOBZbIftwfenjrTVFtKbqCxLQSy1uwp0Ts465havFA4evZA',0)");

		for (int i = 1; i < numberOfAccounts; ++i) {

			sb.append(",('4nXafmMupp7we06EwP6O',0,").append((int) (Math.random() * numberOfBranches)).append(",'CbIa5jNQnTQ1kWlzrhi5EUOBZbIftwfenjrTVFtKbqCxLQSy1uwp0Ts465havFA4evZA',").append(i)
					.append(")");

			if (sb.length() >= 99500) {
				sb.append(";");
				stmt.executeUpdate(sb.toString());
				sb.delete(0, sb.length());
				sb.append("insert into accounts (name, balance, branchid, address, accid) values ('4nXafmMupp7we06EwP6O',0,").append(((int) Math.random() * numberOfBranches)).append(",'',")
						.append(++i).append(")");
			}
		}
		stmt.executeUpdate(sb.toString());
	}

	/**
	 * Füllt die Tellers-Tabelle mit Testtupeln Operiert analog zur
	 * fillAccounts()-Methode mit dem Unterschied, dass der String-Builder nach
	 * und nach Batches erzeugt und anschließend alle Batches auf einmal
	 * ausführt.
	 * 
	 * @throws SQLException
	 */
	private static void fillTellers() throws SQLException {
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

	/**
	 * Füllt die Accounts-Tabelle mit Testtupeln Operiert analog zur
	 * fillAccounts()-Methode mit dem Unterschied, dass der String-Builder nach
	 * und nach Batches erzeugt und anschließend alle Batches auf einmal
	 * ausführt.
	 * 
	 * @throws SQLException
	 */
	private static void fillBranches() throws SQLException {
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

	/**
	 * Methode zum Aufbau der Datenbankverbindung Fragt die Informationen zum
	 * Aufbauen der Datenbank nacheinander ab und versucht danach, die
	 * Verbindung zu initiieren.
	 * 
	 * @param sc
	 *            Scanner für die Eingabe
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static void getConnectionInfo(Scanner sc) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		System.out.print("Verbindung herstellen:\nNetzwerkadresse und Port eingeben\n(Mit : getrennt) :\t\t");
		String ip = sc.next();
		Benchmark.ip = ip;
		System.out.print("Datenbankname eingeben:\t\t");
		String databaseName = sc.next();
		Benchmark.database = databaseName;
		System.out.print("Login-Name eingeben:\t\t");
		String loginName = sc.next();
		Benchmark.username = loginName;
		System.out.print("Passwort eingeben:\t\t");
		String password = sc.next();
		Benchmark.password = password;
		System.out.print("Verbinde: ");
		conn = getConnection("jdbc:mysql://" + ip + "/" + databaseName, loginName, password);
		System.out.println("Fertig!\n");
	}

	/**
	 * Methode zum Durchführen eines Einfüge-Benchmark nach Aufgabe 1 Fragt
	 * zunächst die Anzahl der Branches ab (n) und führt anschließend den
	 * Benchmark aus
	 * 
	 * @param sc
	 * @throws SQLException
	 */
	private static void getBenchmarkInfoAndRun(Scanner sc) throws SQLException {
		System.out.print("Anzahl der BRANCHES angeben: ");
		numberOfBranches = sc.nextInt();
		if (numberOfBranches < 1) {
			System.out.println("\nBe serious!");
			return;
		}
		numberOfAccounts = numberOfBranches * 100000;
		numberOfTellers = numberOfBranches * 10;

		conn.setAutoCommit(false);
		long starttime = System.currentTimeMillis();
		fillDb();
		conn.commit();
		long endtime = System.currentTimeMillis();
		long time = endtime - starttime;

		System.out.println("\nGesamtzeit: " + time + "ms = " + (float) (time / 1000f) + "s = " + (float) (time / 60000f) + "min.\n");

		conn.setAutoCommit(true);

		System.out.print("Soll die Datenbank zurückgesetzt werden?(j/n)");
		if (sc.next().equals("j")) {
			try {
				System.out.print("Setze Datenbank zurück: ");
				reInitDb();
				System.out.println("Fertig!\n");
			} catch (Exception e) {
				System.out.println("Zurücksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
			}
		} else {
			System.out.println("Datenbank bleibt bestehen\n");
		}

	}
}
