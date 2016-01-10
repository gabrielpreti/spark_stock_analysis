package preti.spark.stock.reporting;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import preti.spark.stock.system.TradeSystem;

public class BalanceReport extends AbstractReport {
	@SuppressWarnings("serial")
	public class BalanceEvent implements Serializable {

		private String indexName;
		private Date date;
		private Double balance;

		
		public String getIndexName() {
			return indexName;
		}

		public String getDate() {
			return new SimpleDateFormat("yyyyMMdd").format(date);
		}

		public Double getBalance() {
			return balance;
		}

		public BalanceEvent(Date d, Double balance, String indexName) {
			this.date = d;
			this.balance = balance;
			this.indexName = indexName;
		}

	}

	public BalanceReport(TradeSystem system, String outputIp, int outputPort, String indexName) {
		super(system, outputIp, outputPort, indexName);
	}

	public void executeReport() throws IOException {
		log.info("Generating balance report ");
		Socket socket = new Socket(outputIp, outputPort);
		OutputStream stream = socket.getOutputStream();
		PrintWriter writer = new PrintWriter(stream, true);

		ObjectMapper mapper = new ObjectMapper();

		Map<Date, Double> balanceHistory = system.getBalanceHistory();
		for (Date d : balanceHistory.keySet()) {
			writer.println(mapper.writeValueAsString(new BalanceEvent(d, balanceHistory.get(d), this.indexName)));
		}
		writer.close();
		socket.close();
	}

}
