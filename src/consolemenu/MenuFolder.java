package consolemenu;

import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;

public class MenuFolder extends MenuEntry {

	private ArrayList<MenuEntry> entries;
	protected MenuFolder parent;

	public MenuFolder(String name) {
		this.name = name;
		this.parent = null;
		entries = new ArrayList<MenuEntry>();
	}

	public void add(MenuEntry entry) {
		if (entry instanceof MenuFolder) {
			MenuFolder folderEntry = (MenuFolder) entry;
			folderEntry.parent = this;
		}
		entries.add(entry);
	}

	public void add(MenuEntry[] array) {
		for (MenuEntry entry : array) {
			if (entry instanceof MenuFolder) {
				MenuFolder folderEntry = (MenuFolder) entry;
				folderEntry.parent = this;
			}
			entries.add(entry);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getName() + ":\n");
		for (int i = 0; i < entries.size(); ++i) {
			if (entries.get(i) instanceof MenuFolder)
				sb.append("[" + (i + 1) + "] " + entries.get(i).getName() + "...\n");
			else
				sb.append("[" + (i + 1) + "] " + entries.get(i).getName() + "\n");
		}
		if (this.parent == null)
			sb.append("[" + (entries.size() + 1) + "] Exit\n");
		else
			sb.append("[" + (entries.size() + 1) + "] <<\n");
		return sb.toString();

	}

	/**
	 * prints the result of toString() to the standard output
	 */
	public void print() {
		System.out.print(this.toString());
	}

	/**
	 * Start the user interaction menu starting from this folder
	 */
	@SuppressWarnings("resource")
	public void getUserInput() {
		int eingabe;
		while (true) {

			this.print();

			System.out.print("\nBitte eingeben: ");
			Scanner sc = new Scanner(System.in);
			try {
				eingabe = sc.nextInt();
			} catch (InputMismatchException e) {
				System.out.println("Keine gültige Eingabe\n");
				continue;
			}

			if (eingabe < 1 || eingabe > entries.size() + 1) {
				System.out.println("Keine gültige Eingabe\n");
				continue;
			}

			if (eingabe == entries.size() + 1) {
				return;
			}

			MenuEntry entry = entries.get(eingabe - 1);

			if (entry instanceof MenuFolder) {
				System.out.println();
				((MenuFolder) entry).getUserInput();
				System.out.println();
			}

			if (entry instanceof MenuOption) {
				((MenuOption) entry).perform();
				System.out.println();
			}
		}
	}
}
