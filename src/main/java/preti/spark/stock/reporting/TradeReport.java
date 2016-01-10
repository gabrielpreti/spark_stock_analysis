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

import preti.spark.stock.model.StockTrade;
import preti.spark.stock.model.Trade;
import preti.spark.stock.system.TradeSystem;

public class TradeReport extends AbstractReport {

	@SuppressWarnings("serial")
	public class TradeEvent implements Serializable {

		private String indexName;
		private Date buyDate, sellDate;
		private String stockCode;
		private double size, stopPos;

		public TradeEvent(Date buyDate, Date sellDate, String stockCode, double size, double stopPos,
				String indexName) {
			super();
			this.indexName = indexName;
			this.buyDate = buyDate;
			this.sellDate = sellDate;
			this.stockCode = stockCode;
			this.size = size;
			this.stopPos = stopPos;
		}

		public String getIndexName() {
			return indexName;
		}

		public String getDate() {
			return new SimpleDateFormat("yyyyMMdd").format(buyDate);
		}

		public Date getBuyDate() {
			return buyDate;
		}

		public Date getSellDate() {
			return sellDate;
		}

		public String getStockCode() {
			return stockCode;
		}

		public double getSize() {
			return size;
		}

		public double getStopPos() {
			return stopPos;
		}

	}

	public TradeReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super(system, outputIp, outputPort, indexName);
	}

	@Override
	protected void executeReport() throws IOException {
		log.info("Generating trade report ");
		Socket socket = new Socket(outputIp, outputPort);
		OutputStream stream = socket.getOutputStream();
		PrintWriter writer = new PrintWriter(stream, true);

		ObjectMapper mapper = new ObjectMapper();

		Collection<StockTrade> wallet = system.getWallet();
		for (StockTrade st : wallet) {
			for (Trade t : st.getTrades()) {
				log.info("Generating report for trade " + t);
				writer.println(mapper.writeValueAsString(new TradeEvent(t.getBuyDate(), t.getSellDate(),
						t.getStock().getCode(), t.getSize(), t.getStopPos(), this.indexName)));
			}
		}
		writer.close();
		socket.close();

	}

}
