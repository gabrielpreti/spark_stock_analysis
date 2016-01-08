package preti.spark.stock.system;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import eu.verdelhan.ta4j.Tick;
import eu.verdelhan.ta4j.TimeSeries;
import eu.verdelhan.ta4j.indicators.helpers.HighestValueIndicator;
import eu.verdelhan.ta4j.indicators.helpers.LowestValueIndicator;
import eu.verdelhan.ta4j.indicators.simple.ClosePriceIndicator;
import eu.verdelhan.ta4j.indicators.simple.MaxPriceIndicator;
import eu.verdelhan.ta4j.indicators.simple.MinPriceIndicator;
import preti.spark.stock.model.Stock;
import preti.spark.stock.model.StockHistory;

public class TradingStrategyImpl implements TradingStrategy {
	private static final Log log = LogFactory.getLog(TradingStrategyImpl.class);

	private static final double RISK_FACTOR = 0.02;
	private LowestValueIndicator lowestValueIndicator;
	private HighestValueIndicator highestValueIndicator;

	private int entryDonchianSize, exitDonchianSize;
	private double accountInitialPosition;

	private Stock stock;

	public TradingStrategyImpl(Stock stock, int entryDonchianSize, int exitDonchianSize,
			double accountInitialPosition) {
		super();
		this.entryDonchianSize = entryDonchianSize;
		this.exitDonchianSize = exitDonchianSize;
		this.accountInitialPosition = accountInitialPosition;
		this.stock = stock;

		this.lowestValueIndicator = createLowestValueIndicator();
		this.highestValueIndicator = createHighestValueIndicator();
	}

	public int getEntryDonchianSize() {
		return entryDonchianSize;
	}

	public int getExitDonchianSize() {
		return exitDonchianSize;
	}

	public double getAccountInitialPosition() {
		return accountInitialPosition;
	}

	public Stock getStock() {
		return stock;
	}

	private TimeSeries createTimeSeries() {
		TimeSeries stockHistory = new TimeSeries(stock.getCode(), new ArrayList<>());
		for (Date d : stock.getAllHistoryDates()) {
			StockHistory h = stock.getHistory(d);
			stockHistory.addTick(
					new Tick(new DateTime(d.getTime()), 0, h.getHigh(), h.getLow(), h.getClose(), h.getVolume()));
		}
		return stockHistory;
	}

	private LowestValueIndicator createLowestValueIndicator() {
		MinPriceIndicator minPrice = new MinPriceIndicator(createTimeSeries());
		return new LowestValueIndicator(minPrice, exitDonchianSize);
	}

	private HighestValueIndicator createHighestValueIndicator() {
		MaxPriceIndicator maxPrice = new MaxPriceIndicator(createTimeSeries());
		return new HighestValueIndicator(maxPrice, entryDonchianSize);
	}

	@Override
	public boolean enterPosition(Date d) {
		if (entryDonchianSize == 0) {
			log.info("Skiping at date " + d);
			return false;
		}

		int dataSize = stock.getHistorySizeBeforeDate(d);
		if (dataSize <= entryDonchianSize){
			log.info("Skiping at date " + d);
			return false;
		}
		
		if(d.getMonth()==6 && d.getDate()==22){
			log.info(String.format("date=%s volume=%s close=%s highestValueIndicator=%s", d, stock.getVolumeAtDate(d), stock.getCloseValueAtDate(d), highestValueIndicator.getValue(dataSize-1)));
		}
		
		return stock.getVolumeAtDate(d) >= Math.pow(10, 6)
				&& stock.getCloseValueAtDate(d) > highestValueIndicator.getValue(dataSize - 1).toDouble();
	}

	@Override
	public boolean exitPosition(Date d) {
		int dataSize = stock.getHistorySizeBeforeDate(d);
		if (dataSize <= exitDonchianSize)
			return false;

		return stock.getCloseValueAtDate(d) <= lowestValueIndicator.getValue(dataSize - 1).toDouble();
	}

	@Override
	public double calculatePositionSize(Date d) {
		double stopLossPoint = calculateStopLossPoint(d);
		double stockValue = stock.getCloseValueAtDate(d);

		// log.info(String.format("code=%s accountInitialPosition=%s
		// risck_factor=%s stock_value=%s stopLossPoint=%s",
		// stockTrade.getStock().getCode(), accountInitialPosition, RISK_FACTOR,
		// stockValue, stopLossPoint));
		return Math.floor((accountInitialPosition * RISK_FACTOR) / (stockValue - stopLossPoint));

	}

	@Override
	public double calculateStopLossPoint(Date d) {
		int dataSize = stock.getHistorySizeBeforeDate(d);
		return lowestValueIndicator.getValue(dataSize - 1).toDouble();
	}

	public static void main(String[] args) throws ParseException {
		TimeSeries stockHistory = new TimeSeries("OIBR4", new ArrayList<>());
		stockHistory.addTick(
				new Tick(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-01")), 0, 10, 8, 12, 30));
		stockHistory.addTick(
				new Tick(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-02")), 0, 11, 9, 11, 31));
		stockHistory.addTick(
				new Tick(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-03")), 0, 12, 10, 10, 32));
		stockHistory.addTick(
				new Tick(new DateTime(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-04")), 0, 13, 11, 9, 33));
		ClosePriceIndicator closeIndicator = new ClosePriceIndicator(stockHistory);
		HighestValueIndicator highIndicator = new HighestValueIndicator(closeIndicator, 2);
		System.out.println(highIndicator.getValue(2).toDouble());

	}

}
