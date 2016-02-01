package preti.spark.stock.reporting;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.codehaus.jackson.map.ObjectMapper;

import preti.spark.stock.model.Stock;
import preti.spark.stock.model.StockHistory;
import preti.spark.stock.model.StockTrade;
import preti.spark.stock.model.Trade;
import preti.spark.stock.system.TradeSystem;

public class AggregatedReport extends AbstractReport {

	@SuppressWarnings("serial")
	public class AggregatedEvent implements Serializable {
		private String indexName;
		private Date date;
		private String code;
		private Double closeValue, buyValue, sellValue, size, stopPos;

		public AggregatedEvent(String indexName, Date date, Double closeValue, Double buyValue, Double sellValue,
				Double size, Double stopPos, String code) {
			super();
			this.indexName = indexName;
			this.date = date;
			this.closeValue = closeValue;
			this.buyValue = buyValue;
			this.sellValue = sellValue;
			this.size = size;
			this.stopPos = stopPos;
			this.code = code;
		}

		public String getIndexName() {
			return indexName;
		}

		public String getDate() {
			return new SimpleDateFormat("yyyyMMdd").format(date);
		}

		public Double getCloseValue() {
			return closeValue;
		}

		public Double getBuyValue() {
			return buyValue;
		}

		public Double getSellValue() {
			return sellValue;
		}

		public Double getSize() {
			return size;
		}

		public Double getStopPos() {
			return stopPos;
		}

		public String getCode() {
			return code;
		}

		// public double getProfit() {
		// return sellValue - buyValue;
		// }
		//
		// public boolean isProffitable() {
		// return getProfit() > 0;
		// }

		public long getUnixTimestamp() {
			return date.getTime() / 1000l;
		}

	}

	public AggregatedReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super(system, outputIp, outputPort, indexName);
	}

	@Override
	protected void executeReport() throws IOException {
		log.info("Generating aggregated report ");
		Socket socket = new Socket(outputIp, outputPort);
		OutputStream stream = socket.getOutputStream();
		PrintWriter writer = new PrintWriter(stream, true);

		ObjectMapper mapper = new ObjectMapper();

		Collection<StockTrade> wallet = system.getWallet();
		for (StockTrade st : wallet) {
			Stock stock = st.getStock();
			for (Date d : stock.getAllHistoryDates()) {
				Double closeValue = null;
				Double buyValue = null;
				Double sellValue = null;
				Double size = null;
				Double stopPos = null;

				StockHistory history = stock.getHistory(d);
				closeValue = history.getClose();

				Trade openingTrade = st.getTradeOpenAt(d);
				if (openingTrade != null) {
					buyValue = openingTrade.getBuyValue();
					size = openingTrade.getSize();
					stopPos = openingTrade.getStopPos();
				}

				Trade closingTrade = st.getTradeClosedAt(d);
				if (closingTrade != null) {
					sellValue = closingTrade.getSellValue();
					// buyValue = closingTrade.getBuyValue();
					size = closingTrade.getSize();
					stopPos = closingTrade.getStopPos();
				}

				writer.println((mapper.writeValueAsString(new AggregatedEvent(indexName, d, closeValue, buyValue,
						sellValue, size, stopPos, stock.getCode()))));
			}
		}

		writer.close();
		socket.close();

	}

}
