package preti.spark.stock;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.esotericsoftware.minlog.Log;

@SuppressWarnings("serial")
public class InputDataEntry implements Serializable {

	private Date date;
	private String code;
	private String type;
	private String name;
	private double open;
	private double high;
	private double low;
	private double close;
	private double volume;

	public InputDataEntry(Date date, String code, String type, String name, double open, double high, double low,
			double close, double volume) {
		super();
		this.date = date;
		this.code = code;
		this.type = type;
		this.name = name;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.volume = volume;
	}

	public Date getDate() {
		return date;
	}

	public String getCode() {
		return code;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public double getOpen() {
		return open;
	}

	public double getHigh() {
		return high;
	}

	public double getLow() {
		return low;
	}

	public double getClose() {
		return close;
	}

	public double getVolume() {
		return volume;
	}

	public String toString() {
		return String.format("date=%s code=%s type=%s name=%s open=%s high=%s low=%s close=%s volume=%s", date, code,
				type, name, open, high, low, close, volume);
	}

	private static String INPUT_PATTERN = "\\d\\d(?<strdate>\\d{8})..(?<code>.{12})(?<type>.{3})(?<name>.{12}).{17}(?<open>\\d{13})(?<high>\\d{13})(?<low>\\d{13}).{13}(?<close>\\d{13}).{49}(?<volume>\\d{18})";
	private static String DATE_PATTERN = "yyyyMMdd";
	private static Pattern pattern = Pattern.compile(INPUT_PATTERN);
	
	public static InputDataEntry parseFromLine(String line) throws ParseException{
		Matcher m = pattern.matcher(line);
		m.find();
		Date date = new SimpleDateFormat(DATE_PATTERN).parse(m.group("strdate"));
		String code = m.group("code").trim();
		String type = m.group("type").trim();
		String name = m.group("name").trim();
		double open = NumberFormat.getInstance().parse(m.group("open").trim()).doubleValue()/100;
		double high = NumberFormat.getInstance().parse(m.group("high").trim()).doubleValue()/100;
		double low = NumberFormat.getInstance().parse(m.group("low").trim()).doubleValue()/100;
		double close = NumberFormat.getInstance().parse(m.group("close").trim()).doubleValue()/100;
		double volume = NumberFormat.getInstance().parse(m.group("volume").trim()).doubleValue()/100;
		
		return new InputDataEntry(date, code, type, name, open, high, low, close, volume);
	}

	public static void main(String[] args) throws IOException, ParseException {
		String inputFile = "/tmp/cotacoes2.txt";
		

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line;
		while ((line = reader.readLine()) != null) {
			if(line.trim().isEmpty())
				continue;
			System.out.println(InputDataEntry.parseFromLine(line));
		}
		reader.close();
	}

}
