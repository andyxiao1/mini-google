package edu.upenn.cis.cis455.xpathengine;

/**
 * This class encapsulates the tokens we care about parsing in XML (or HTML)
 */
public class OccurrenceEvent {
	public enum Type {
		Open, Close, Text
	};

	String url;
	Type type;
	String value;
	boolean isHtml;

	public OccurrenceEvent(Type t, String value, String urlStr, boolean isHtmlDoc) {
		this.type = t;
		this.value = value;
		url = urlStr;
		isHtml = isHtmlDoc;
	}

	public String getUrl() {
		return url;
	}

	public boolean getIsHtml() {
		return isHtml;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getValue() {
		return isHtml ? value.toLowerCase() : value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String toString() {
		if (type == Type.Open)
			return "<" + value + ">";
		else if (type == Type.Close)
			return "</" + value + ">";
		else
			return value;
	}
}
