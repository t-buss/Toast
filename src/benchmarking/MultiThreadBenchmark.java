package benchmarking;
import java.io.IOException;
import java.util.ArrayList;

public class MultiThreadBenchmark {

	public static StringBuilder sbl = new StringBuilder();

	public MultiThreadBenchmark() {

	}
	public void run(String address, String username, String password, int threadcount) throws InterruptedException, IOException {
		
		int WARMUPTIME = 4;
		int MEASURINGTIME = 5;
		int COOLDOWNTIME = 1;
		
		System.out.println("Die Gesamtzeit wird " + (WARMUPTIME + MEASURINGTIME + COOLDOWNTIME) + " Minuten betragen");
		
		ArrayList<Thread> threads = new ArrayList<Thread>();
		ArrayList<LoadDriver> drivers = new ArrayList<LoadDriver>();

		for (int i = 0; i < threadcount; ++i) {
			try {
				drivers.add(new LoadDriver(address, username, password));
			} catch (Exception e) {
				System.out.println("[FAILED]\nLoadDriver konnten nicht erstellt werden: " + e.getLocalizedMessage());
				e.printStackTrace();
				return;
			}
		}
		for (int i = 0; i < threadcount; i++) {
			threads.add(new Thread(drivers.get(i)));
			threads.get(i).start();
		};

		System.out.println("Beginne mit Warmup-Phase (" + WARMUPTIME + "min): ");
		for(int i=WARMUPTIME; i>0;--i) {
			System.out.println("Noch " + i  + " min.");
			Thread.sleep(60000);
		}

		// Werte aus den Threads holen:
		int sumBeforeMeasuring = 0;
		for (int i = 0; i < threadcount; i++) {
			sumBeforeMeasuring += drivers.get(i).numberOfTransactions;
		}
		System.out.println("Transaktionen vor der Messung: " + sumBeforeMeasuring);

		System.out.println("Beginne mit Mess-Phase (" + MEASURINGTIME + "min): ");
		for(int i=MEASURINGTIME; i>0;--i) {
			System.out.println("Noch " + i  + " min.");
			Thread.sleep(60000);
		}

		// Nochmal die Daten aus den LoadDrivern holen, rechnen
		int sumAfterMeasuring = 0;
		for (int i = 0; i < threadcount; i++) {
			sumAfterMeasuring += drivers.get(i).numberOfTransactions;
		}
		System.out.println("Transaktionen nach der Messung: " + sumAfterMeasuring);
		System.out.println("Durchschnittliche Anzahl Transaktionen: " + (float) (sumAfterMeasuring - sumBeforeMeasuring) / (MEASURINGTIME * 60f) + " 1/s");

		sbl.append(Logfile.getDate() + "\n");
		sbl.append("Anzahl der Threads: "+ threadcount + "\n" + "Transaktionen vor der Messung: " + sumBeforeMeasuring + "\n" 
				+ "Transaktionen nach der Messung: " + sumAfterMeasuring + "\n" + "Durchschnittliche Anzahl Transaktionen: "
				+ (float) (sumAfterMeasuring - sumBeforeMeasuring) / (MEASURINGTIME * 60f) + " 1/s");

		System.out.println("Beginne mit Cooldown-Phase (" + COOLDOWNTIME + "min): ");
		for(int i=COOLDOWNTIME; i>0;--i) {
			System.out.println("Noch " + i  + " min.");
			Thread.sleep(60000);
		}

		for (int i = 0; i < threadcount; i++) {
			threads.get(i).interrupt();
		}

		Logfile.updateLog(sbl.toString());
		sbl.delete(0, sbl.length());

	}
}
