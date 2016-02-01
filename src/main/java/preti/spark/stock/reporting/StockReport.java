package preti.spark.stock.reporting;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import preti.spark.stock.model.Stock;
import preti.spark.stock.model.StockHistory;
import preti.spark.stock.system.TradeSystem;

public class StockReport extends AbstractReport {

	@SuppressWarnings("serial")
	public class StockHistoryEvent implements Serializable {

		private String indexName;
		private String code;
		private Date date;
		private double high, low, close, volume;

		public StockHistoryEvent(String code, Date date, double high, double low, double close, double volume, String indexName) {
			super();
			this.code = code;
			this.date = date;
			this.high = high;
			this.low = low;
			this.close = close;
			this.volume = volume;
			this.indexName = indexName;
		}

		public String getIndexName() {
			return indexName;
		}
		
		public String getCode() {
			return code;
		}

		public String getDate() {
			return new SimpleDateFormat("yyyyMMdd").format(date);
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

		public long getUnixTimestamp() {
			return date.getTime() / 1000l;
		}

	}

	public StockReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super(system, outputIp, outputPort, indexName);
	}

	public void executeReport() throws IOException {
		log.info("Generating stock report ");
		Socket socket = new Socket(outputIp, outputPort);
		OutputStream stream = socket.getOutputStream();
		PrintWriter writer = new PrintWriter(stream, true);

		ObjectMapper mapper = new ObjectMapper();

		List<Stock> stocks = system.getStocks();
		for (Stock s : stocks) {
			for (Date d : s.getAllHistoryDates()) {
				StockHistory history = s.getHistory(d);
				writer.println(mapper.writeValueAsString(new StockHistoryEvent(s.getCode(), d, history.getHigh(), history.getLow(),
						history.getClose(), history.getVolume(), this.indexName)));
			}
		}
		writer.close();
		socket.close();
	}

}
