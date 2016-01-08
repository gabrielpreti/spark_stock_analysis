package preti.spark.stock.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("serial")
public class Stock implements Serializable {
	private String code;
	private TreeMap<Date, StockHistory> history = new TreeMap<>();

	public Stock(String code) {
		super();
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	public Map<Date, StockHistory> getHistory() {
		return history;
	}

	public Set<Date> getAllHistoryDates() {
		return history.keySet();
	}

	public void addHistory(StockHistory h) {
		history.put(h.getDate(), h);
	}

	public StockHistory getHistory(Date d) {
		return history.get(d);
	}

	public double getCloseValueAtDate(Date d) {
		return history.floorEntry(d).getValue().getClose();
	}
	
	public double getVolumeAtDate(Date d) {
		return history.floorEntry(d).getValue().getVolume();
	}

	public boolean hasHistoryAtDate(Date d) {
		return history.containsKey(d);
	}
	
	public int getHistorySizeBeforeDate(Date d){
		return history.headMap(d).size();
	}

}
