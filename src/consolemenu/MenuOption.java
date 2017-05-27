package consolemenu;

public abstract class MenuOption extends MenuEntry {

	public MenuOption(String name) {
		this.name = name;
	}

	public abstract void perform();
}
