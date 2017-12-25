package imdb_spark.functionality;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * Created by zorka_000 on 25.12.2017.
 */

public interface OperationInterface {

    String description = null;
    public String getDescription();
    Dataset<Row> doWork(Dataset<Row> dataFrame);
}
