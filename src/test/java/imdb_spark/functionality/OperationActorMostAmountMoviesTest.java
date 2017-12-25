package imdb_spark.functionality;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * Created by zorka_000 on 25.12.2017.
 */
public class OperationActorMostAmountMoviesTest {
    @Autowired
    private SQLContext sqlContext;
    @Autowired
    private OperationActorMostAmountMovies mostMoviesActor;
    @Test
    public void doWork() throws Exception {
        Dataset<Row> dataFrame = sqlContext.read().format("csv").option("header", "true").option("delimiter",";").load("./data/title.basics-test.csv");
        Dataset<Row> result = mostMoviesActor.doWork(dataFrame);
        //result.first().getInt(0)
        //Assert.assertEquals("java",result.get(0));
    }

}