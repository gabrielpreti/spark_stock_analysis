package preti.spark.stock.system;

import java.util.Date;

public interface TradingStrategy {

	boolean enterPosition(Date d);

	boolean exitPosition(Date d);

	double calculatePositionSize(Date d);

	double calculateStopLossPoint(Date d);

}
