package consolemenu;

public class MenuTest {

	public static void main(String[] args) {
		MenuFolder root = new MenuFolder("Hauptmenü");
		
		root.add(new MenuOption("Eine Option") {
			public void perform() {
				System.out.println("Ich bin eine Ausgabe!");
			}
		});
		
		MenuFolder folder1 = new MenuFolder("Ein Ordner");
		
		folder1.add(new MenuOption("Eine Unter-Option") {

			@Override
			public void perform() {
				System.out.println(Math.random());

			}
		});

		folder1.add(new MenuOption("Eine andere Unter-Option") {

			@Override
			public void perform() {
				System.out.println("Hier könnte Ihre Werbung stehen");
			}
		});
		
		root.add(folder1);

		root.getUserInput();
		
		System.out.println("\n\n<<<ENF OF LINE>>>\n\n");
	}
}
