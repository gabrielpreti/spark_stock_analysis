package preti.spark.stock.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SuppressWarnings("serial")
public class StockTrade implements Serializable {
	private Stock stock;
	private List<Trade> trades;

	public StockTrade(Stock stock) {
		super();
		this.stock = stock;
		trades = new ArrayList<>();
	}

	public Stock getStock() {
		return stock;
	}
	
	public Trade getLastTrade() {
		if (trades.size() == 0)
			return null;

		return trades.get(trades.size() - 1);
	}

	public boolean isInOpenPosition() {
		Trade t = getLastTrade();
		return t != null && t.isOpen();
	}

	public Trade openNewTrade(double size, Date buyDate, double stopPos) {
		if (isInOpenPosition()) {
			throw new IllegalArgumentException("Can't open a new trade with one already opened.");
		}

		Trade t = new Trade(this.stock, size, stopPos, buyDate);
		trades.add(t);
		return t;
	}

	public Trade closeLastTrade(Date closeDate) {
		if (!isInOpenPosition()) {
			throw new IllegalArgumentException("No open trade to close.");
		}
		Trade t = getLastTrade();
		t.close(closeDate);
		return t;
	}

	public boolean hasReachedStopPosition(Date d) {
		Trade t = getLastTrade();
		return t != null && t.isOpen() && t.hasReachedStopPosition(d);
	}

	public boolean isProfittable(Date d) {
		Trade t = getLastTrade();
		return t != null && t.isProfitable(d);
	}

	public boolean hasAnyTrade() {
		return trades.size() > 0;
	}

}
