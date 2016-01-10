package preti.spark.stock.system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import preti.spark.stock.model.Stock;
import preti.spark.stock.model.StockTrade;
import preti.spark.stock.model.Trade;

@SuppressWarnings("serial")
public class TradeSystem implements Serializable {
	private static final Log log = LogFactory.getLog(TradeSystem.class);

	private Collection<StockTrade> wallet;
	private double accountInitialPosition;
	private double accountBalance;
	private Map<Date, Double> balanceHistory;

	private Map<String, TradingStrategy> tradingStrategies;

	public TradeSystem(Stock stock, double accountInitialPosition, TradingStrategy strategy) {
		this(Arrays.asList(stock), accountInitialPosition, null);
		this.tradingStrategies = new HashMap<>();
		tradingStrategies.put(stock.getCode(), strategy);
	}

	public TradeSystem(Collection<Stock> stocks, double accountInitialPosition,
			Map<String, TradingStrategy> tradingStrategies) {
		this.accountInitialPosition = accountInitialPosition;
		this.accountBalance = accountInitialPosition;

		if (stocks == null || stocks.isEmpty()) {
			throw new IllegalArgumentException("No stocks to analyze.");
		}

		wallet = new HashSet<>();
		for (Stock s : stocks) {
			wallet.add(new StockTrade(s));
		}

		balanceHistory = new TreeMap<>();
		this.tradingStrategies = tradingStrategies;
	}

	public Map<Date, Double> getBalanceHistory() {
		return balanceHistory;
	}

	public Collection<StockTrade> getWallet() {
		return wallet;
	}

	public List<Stock> getStocks() {
		List<Stock> stocks = new ArrayList<>();
		for (StockTrade st : wallet) {
			stocks.add(st.getStock());
		}
		return stocks;
	}

	public void setTradingStrategies(Map<String, TradingStrategy> strategies) {
		this.tradingStrategies = strategies;
	}

	public void applyTradingStrategy(String stockCode, TradingStrategy strategy) {
		this.tradingStrategies.put(stockCode, strategy);
	}

	private boolean openNewTrade(StockTrade stockTrade, Date d) {
		TradingStrategy strategy = this.tradingStrategies.get(stockTrade.getStock().getCode());
		double size = strategy.calculatePositionSize(d);
		if (size < 1) {
			log.info("Postion size<1: not enough balance to enter position");
			return false;
		}

		double stockValue = stockTrade.getStock().getCloseValueAtDate(d);
		while ((size * stockValue) > this.accountBalance && size > 1) {
			size--;
		}
		if (size < 1) {
			log.info("Not enough balance to enter position");
			return false;
		}

		Trade t = stockTrade.openNewTrade(size, d, strategy.calculateStopLossPoint(d));
		log.info("Opening new trade: " + t);
		this.accountBalance -= t.getSize() * t.getBuyValue();
		return true;
	}

	private void closeLastTrade(StockTrade stockTrade, Date d) {
		Trade t = stockTrade.closeLastTrade(d);
		this.accountBalance += t.getSize() * t.getSellValue();
		log.info("Closing trade " + t);
	}

	private double calculateTotalOpenPositions(Date d) {
		double total = 0;
		for (StockTrade st : wallet) {
			if (st.isInOpenPosition()) {
				Trade openTrade = st.getLastTrade();
				total += openTrade.getSize() * openTrade.getStock().getCloseValueAtDate(d);
			}
		}
		return total;
	}

	public double getAccountInitialPosition() {
		return accountInitialPosition;
	}

	public double getAccountBalance() {
		return accountBalance;
	}

	public void analyzeStocks(Date initialDate, Date finalDate) {
		// Identifica todas as datas, de forma unica e ordenada
		TreeSet<Date> allDates = new TreeSet<>();
		for (StockTrade st : this.wallet) {
			allDates.addAll(st.getStock().getAllHistoryDates());
		}

		// Verifica se foi especificado uma data inicial
		if (initialDate != null) {
			allDates = new TreeSet<Date>(allDates.tailSet(initialDate));
		}

		// Verifica se foi especificado uma data final
		if (finalDate != null) {
			allDates = new TreeSet<Date>(allDates.headSet(finalDate, true));
		}

		for (Date date : allDates) {
			for (StockTrade stockTrade : wallet) {
				if (!stockTrade.getStock().hasHistoryAtDate(date)) {
					continue;
				}

				TradingStrategy strategy = this.tradingStrategies.get(stockTrade.getStock().getCode());
				if (strategy == null) {
					continue;
				}

				if (stockTrade.isInOpenPosition()) {
					boolean profittable = stockTrade.isProfittable(date);
					if ((profittable && strategy.exitPosition(date))
							|| (!profittable && stockTrade.hasReachedStopPosition(date))) {
						closeLastTrade(stockTrade, date);
					}

				} else {
					if (strategy.enterPosition(date)) {
						openNewTrade(stockTrade, date);
					}
				}
			}
			balanceHistory.put(date, this.accountBalance);
		}
	}

	public void closeAllOpenTrades(Date d) {
		for (StockTrade st : wallet) {
			if (st.isInOpenPosition()) {
				this.closeLastTrade(st, d);
			}
		}
	}

}
