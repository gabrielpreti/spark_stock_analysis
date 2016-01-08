package preti.spark.stock.model;

import java.io.Serializable;
import java.util.Date;

@SuppressWarnings("serial")
public class Trade implements Serializable {
	private Stock stock;
	private double size;
	private double stopPos;
	private Date buyDate;
	private Date sellDate;
	
	public Trade(Stock stock, double size, double stopPos, Date buyDate) {
		this(stock, size, stopPos, buyDate, null);
	}

	public Trade(Stock stock, double size, double stopPos, Date buyDate, Date sellDate) {
		super();
		this.stock = stock;
		this.size = size;
		this.stopPos = stopPos;
		this.buyDate = buyDate;
		this.sellDate = sellDate;
	}

	public Stock getStock() {
		return stock;
	}

	public double getSize() {
		return size;
	}

	public double getStopPos() {
		return stopPos;
	}

	public Date getBuyDate() {
		return buyDate;
	}

	public Date getSellDate() {
		return sellDate;
	}

	public boolean isOpen() {
		return sellDate == null;
	}

	public void close(Date d) {
		this.sellDate = d;
	}

	public double getProfit(Date d) {
		if (buyDate == null) {
			throw new IllegalArgumentException("Trade invalid: buyDate is empty.");
		}

		if (d == null && isOpen()) {
			throw new IllegalArgumentException("Trade not closed and no date informed to calculate profits.");
		}

		if (d != null) {
			return (stock.getCloseValueAtDate(d) - stock.getCloseValueAtDate(buyDate));
		} else {
			return (stock.getCloseValueAtDate(sellDate) - stock.getCloseValueAtDate(buyDate));
		}
	}

	public boolean isProfitable(Date d) {
		return getProfit(d) > 0;
	}

	public double getBuyValue() {
		return stock.getCloseValueAtDate(buyDate);
	}

	public double getSellValue() {
		if (sellDate == null) {
			throw new IllegalArgumentException("No sell value: trade not closed");
		}

		return stock.getCloseValueAtDate(sellDate);
	}

	public boolean hasReachedStopPosition(Date d) {
		if (!isOpen())
			throw new IllegalArgumentException("Trade is not opened.");

		return stock.getCloseValueAtDate(d) <= stopPos;
	}
	
	public String toString() {
		if(isOpen()){
			return String.format("stock=%s size=%s buyValue=%s stopPos=%s buy=Date%s", stock.getCode(), size, getBuyValue(), stopPos, buyDate);
		}else{
			return String.format("stock=%s size=%s buyValue=%s stopPos=%s buy=Date%s sellDate=%s sellValue=%s", stock.getCode(), size, getBuyValue(), stopPos, buyDate, sellDate, stock.getCloseValueAtDate(sellDate));
		}
		
	}

}
