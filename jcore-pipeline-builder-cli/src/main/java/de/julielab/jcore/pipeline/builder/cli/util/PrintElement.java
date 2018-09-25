package de.julielab.jcore.pipeline.builder.cli.util;

public class PrintElement {
    private String text = "";
    private String prefix;

    public PrintElement(String text, String prefix) {

        this.text = text;
        this.prefix = prefix;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

}
