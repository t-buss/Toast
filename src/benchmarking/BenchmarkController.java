package benchmarking;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

import consolemenu.MenuFolder;
import consolemenu.MenuOption;

public class BenchmarkController {

	private static BenchmarkController instance;
	public BenchmarkCapable systemUnderTest;
	private MenuFolder mainmenu;
	private Scanner sc = new Scanner(System.in);

	private Connection conn;
	private String networkAddress, username, password;

	int setNumberOfBranches = 10;
	int setNumberOfLoadDrivers = 5;

	public static BenchmarkController getInstance(String dbms) {
		if (instance == null) {
			instance = new BenchmarkController(dbms, null, null, null);
		}
		return instance;
	}

	private BenchmarkController(String dbms, String networkAddress, String username, String password) {
		instance = this;
		this.networkAddress = networkAddress;
		this.username = username;
		this.password = password;

		try {
			systemUnderTest = (BenchmarkCapable) Class.forName("databases." + dbms).newInstance();
		} catch (InstantiationException e1) {
			System.out.println("Datenbank-Klasse konnte nicht instantiiert werden");
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			System.out.println("Die Operation ist nicht erlaubt");
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			System.out.println("Die angegebene Datenbank-Klasse wurde nicht gefunden");
			e1.printStackTrace();
		}

		if (networkAddress != null && username != null && password != null) {
			conn = systemUnderTest.getConnection(networkAddress, username, password);
			if (conn == null) {
				System.out.println("Konnte keine Verbindung mit Parametern herstellen");
			}
		}

		mainmenu = new MenuFolder("Hauptmenü");
		mainmenu.add(new MenuOption("Verbindung aufbauen") {
			@Override
			public void perform() {
				// Verbindung aufbauen
				initConnection();
			}
		});
		mainmenu.add(new MenuOption("Datenbankstatus") {
			@Override
			public void perform() {
				System.out.print("Datenbank Informationen:\nVerbindung:\t\t");
				if (conn != null) {
					System.out.println("Aufgebaut!");
					try {
						System.out.println("Branches-Tabelle:\t" + systemUnderTest.getNumberOfBranches(conn));
						System.out.println("Accounts-Tabelle:\t" + systemUnderTest.getNumberOfAccounts(conn));
						System.out.println("Tellers-Tabelle:\t" + systemUnderTest.getNumberOfTellers(conn));
						System.out.println("History-Tabelle:\t" + systemUnderTest.getNumberOfHistoryEntries(conn));
					} catch (SQLException e) {

					}
				} else {
					System.out.println("Nicht aufgebaut");
				}
			}
		});
		mainmenu.add(new MenuOption("Datenbank-Typ wählen") {
			@Override
			public void perform() {
				String databasetype = null;
				System.out.println("Verfügbare Datenbanken: ");
				File folder = new File("./bin/databases");
				File[] listOfFiles = folder.listFiles();
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						System.out.print(listOfFiles[i].getName().substring(0, listOfFiles[i].getName().lastIndexOf(".")) + " ");
					}
				}
				System.out.print("\nDatenbank-Typ eingeben: ");
				databasetype = sc.next();
				try {
					systemUnderTest = (BenchmarkCapable) Class.forName("databases." + databasetype).newInstance();
					System.out.println("Datenbank eingestellt\n");
				} catch (InstantiationException e1) {
					System.out.println("Datenbank-Klasse konnte nicht instantiiert werden");
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					System.out.println("Die Operation ist nicht erlaubt");
					e1.printStackTrace();
				} catch (ClassNotFoundException e1) {
					System.out.println("Die angegebene Datenbank-Klasse wurde nicht gefunden");
					e1.printStackTrace();
				}
			}

		});
		mainmenu.add(new MenuOption("Datenbank-Schema initiieren") {
			@Override
			public void perform() {
				// Datenbank-Schema initiieren
				try {
					systemUnderTest.initDbSchema(conn);
				} catch (SQLException e) {
					System.out.println("Failed to init database schema: ");
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht hergestellt");
				}
			}
		});

		MenuFolder deleteOptions = new MenuFolder("Löschen");
		mainmenu.add(deleteOptions);
		deleteOptions.add(new MenuOption("Gesamte Datenbank zurücksetzen") {
			@Override
			public void perform() {
				try {
					systemUnderTest.dropTables(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});
		deleteOptions.add(new MenuOption("Alle Tabellen leeren") {
			@Override
			public void perform() {
				try {
					systemUnderTest.truncateAll(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});
		deleteOptions.add(new MenuOption("BRANCHES leeren") {
			@Override
			public void perform() {
				try {
					systemUnderTest.truncateBranches(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});
		deleteOptions.add(new MenuOption("ACCOUNTS leeren") {
			@Override
			public void perform() {
				try {
					systemUnderTest.truncateAccounts(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});
		deleteOptions.add(new MenuOption("TELLERS leeren") {
			@Override
			public void perform() {
				try {
					systemUnderTest.truncateTellers(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});
		deleteOptions.add(new MenuOption("HISTORY leeren") {
			@Override
			public void perform() {
				try {
					systemUnderTest.truncateHistory(conn);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				}
			}
		});

		MenuFolder insertBenchmarkOptions = new MenuFolder("Einfüge-Benchmark");
		mainmenu.add(insertBenchmarkOptions);
		insertBenchmarkOptions.add(new MenuOption("Benchmark durchführen") {
			@Override
			public void perform() {
				try {
					insertBenchmark(setNumberOfBranches);
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		insertBenchmarkOptions.add(new MenuOption("Anzahl der Branches setzen") {
			@Override
			public void perform() {
				System.out.print("Anzahl eingeben: ");
				setNumberOfBranches = sc.nextInt();
			}
		});

		MenuFolder transBenchmarkOptions = new MenuFolder("Transaktionen-Benchmark");
		mainmenu.add(transBenchmarkOptions);
		transBenchmarkOptions.add(new MenuOption("Benchmark durchführen") {
			@Override
			public void perform() {
				try {
					multiThreadBenchmark(setNumberOfLoadDrivers);
				} catch (NullPointerException e) {
					System.out.println("Verbindung wurde noch nicht aufgebaut");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		transBenchmarkOptions.add(new MenuOption("Anzahl der LoadDriver setzen") {
			@Override
			public void perform() {
				System.out.print("Anzahl eingeben: ");
				setNumberOfLoadDrivers = sc.nextInt();
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
	}

	public void initConnection() {
		System.out.print("Verbindung herstellen:\nAdresse eingeben (ip:port/db) :\t");
		networkAddress = sc.next();
		System.out.print("Login-Name eingeben:\t\t");
		username = sc.next();
		System.out.print("Passwort eingeben:\t\t");
		password = sc.next();
		System.out.print("Verbinde: ");
		conn = systemUnderTest.getConnection(networkAddress, username, password);
		if (conn != null)
			System.out.println("Fertig!\n");
		else
			System.out.println("Fehlgeschlagen\n");
	}

	public void insertBenchmark(int numberOfBranches) throws SQLException, NullPointerException {
		int numberOfAccounts = numberOfBranches * 100000;
		int numberOfTellers = numberOfBranches * 10;
		conn.setAutoCommit(false);
		long starttime = System.currentTimeMillis();
		System.out.print("Lege BRANCHES an...\t");
		systemUnderTest.fillBranches(conn, numberOfBranches);
		System.out.println("Fertig!");
		System.out.print("Lege ACCOUNTS an...\t");
		systemUnderTest.fillAccounts(conn, numberOfAccounts, numberOfBranches);
		System.out.println("Fertig!");
		System.out.print("Lege TELLERS an...\t");
		systemUnderTest.fillTellers(conn, numberOfTellers, numberOfBranches);
		System.out.println("Fertig!");
		conn.commit();
		long endtime = System.currentTimeMillis();
		long time = endtime - starttime;
		System.out.println("\nGesamtzeit: " + time + "ms = " + (float) (time / 1000f) + "s = " + (float) (time / 60000f) + "min.\n");
		conn.setAutoCommit(true);
		System.out.print("Sollen die Tabellen geleert werden?(j/n)");
		if (sc.next().equals("j")) {
			try {
				System.out.print("Setze Datenbank zurück: ");
				systemUnderTest.truncateAll(conn);
				System.out.println("Fertig!\n");
			} catch (Exception e) {
				System.out.println("Zurücksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
			}
		} else {
			System.out.println("Datenbank bleibt bestehen\n");
		}
	}

	public void multiThreadBenchmark(int numberOfLoadDrivers) throws InterruptedException, IOException, NullPointerException {
		new MultiThreadBenchmark().run(networkAddress, username, password, numberOfLoadDrivers);
		System.out.print("Soll die HISTORY-Tabelle zurückgesetzt werden?(j/n)");
		if (sc.next().equals("j")) {
			try {
				System.out.print("Loesche HISTORY... ");
				systemUnderTest.truncateHistory(conn);
				System.out.println("Fertig!\n");
			} catch (Exception e) {
				System.out.println("Zuruecksetzen fehlgeschlagen.\nFehler: " + e.getMessage() + "\n\n");
			}
		} else {
			System.out.println("Tabelle bleibt bestehen\n");
		}
	}

	public static void main(String[] args) {
		System.out.println("Benchmark-Programm v1.3.1\n");
		BenchmarkController myController;

		try {
			if (args.length > 1 && args.length <= 4) {
				myController = new BenchmarkController(args[0], args[1], args[2], args[3]);
			} else {
				myController = new BenchmarkController(args[0], null, null, null);
			}
			myController.mainmenu.getUserInput();
		} catch (IndexOutOfBoundsException | NullPointerException e) {
			System.out.println("Eine Datenbank muss als Parameter angegeben werden:");
			System.out.println("Aufruf: java -jar BenchmarkController <DB-Klasse> (<Adresse> <Username> <Passwort>)");
			System.out.print("Verfügbare Datenbanken: ");
			File folder = new File("./bin/databases");
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					System.out.print(listOfFiles[i].getName().substring(0, listOfFiles[i].getName().lastIndexOf(".")) + " ");
				}
			}
		}
		System.out.println("\n\n<<<END OF LINE>>>");
	}
}
