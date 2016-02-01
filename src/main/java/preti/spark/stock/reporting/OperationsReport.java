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

public class OperationsReport extends AbstractReport {
	public enum OperationType {
		BUY, SELL;
	};

	@SuppressWarnings("serial")
	public class OperationEvent implements Serializable {

		private String indexName;
		private OperationType type;
		private String stockCode;
		private Date date;
		private double size, stopPos, buyValue, sellValue;
		private boolean profitable;

		public OperationEvent(String indexName, OperationType type, String stockCode, Date date, double size,
				double stopPos, double buyValue, double sellValue, boolean profitable) {
			super();
			this.indexName = indexName;
			this.type = type;
			this.stockCode = stockCode;
			this.date = date;
			this.size = size;
			this.stopPos = stopPos;
			this.buyValue = buyValue;
			this.sellValue = sellValue;
			this.profitable = profitable;
		}

		public String getIndexName() {
			return indexName;
		}

		public String getType() {
			return type.toString();
		}

		public String getStockCode() {
			return stockCode;
		}

		public String getDate() {
			return new SimpleDateFormat("yyyyMMdd").format(date);
		}

		public double getSize() {
			return size;
		}

		public double getStopPos() {
			return stopPos;
		}

		public double getValue() {
			if (OperationType.BUY.equals(type)) {
				return buyValue;
			} else {
				return sellValue;
			}
		}

		public boolean isProfitable() {
			return profitable;
		}

		public String getStatus() {
			if (profitable) {
				return "profitable";
			} else {
				return "unprofitable";
			}
		}

		public double getGain() {
			if (OperationType.BUY.equals(type)) {
				return 0;
			} else {
				return (sellValue - buyValue) * size;
			}
		}

		public long getUnixTimestamp() {
			return date.getTime() / 1000l;
		}

	}

	public OperationsReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super(system, outputIp, outputPort, indexName);
	}

	@Override
	protected void executeReport() throws IOException {
		log.info("Generating operations report ");
		Socket socket = new Socket(outputIp, outputPort);
		OutputStream stream = socket.getOutputStream();
		PrintWriter writer = new PrintWriter(stream, true);

		ObjectMapper mapper = new ObjectMapper();

		Collection<StockTrade> wallet = system.getWallet();
		for (StockTrade st : wallet) {
			for (Trade t : st.getTrades()) {
				log.info("Generating report for trade " + t);
				boolean proffitable = t.isProfitable();
				OperationEvent buyEvent = new OperationEvent(this.indexName, OperationType.BUY, t.getStock().getCode(),
						t.getBuyDate(), t.getSize(), t.getStopPos(), t.getBuyValue(), t.getSellValue(), proffitable);
				OperationEvent sellEvent = new OperationEvent(this.indexName, OperationType.SELL,
						t.getStock().getCode(), t.getSellDate(), t.getSize(), t.getStopPos(), t.getBuyValue(), t.getSellValue(),
						proffitable);
				writer.println(mapper.writeValueAsString(buyEvent));
				writer.println(mapper.writeValueAsString(sellEvent));
			}
		}
		writer.close();
		socket.close();

	}

}
