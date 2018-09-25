package de.julielab.jcore.pipeline.builder.cli.menu;

import java.util.ArrayList;
import java.util.List;

public class MenuItemList <T extends IMenuItem> extends ArrayList<T>  {
    public List<String> getMenuItemNames() {
        List<String> menuItems = new ArrayList<>();
        for (int i = 0; i < size(); ++i)
            menuItems.add((i+1) + ") " + get(i).getName());
        return menuItems;
    }
}
