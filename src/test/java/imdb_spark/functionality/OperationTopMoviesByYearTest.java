package imdb_spark.functionality;

import imdb_spark.Const;
import imdb_spark.configuration.Conf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * Created by zorka_000 on 26.12.2017.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Conf.class)
@ActiveProfiles(Const.DEV)
public class OperationTopMoviesByYearTest {
    @Autowired
    private SQLContext sqlContext;
    @Autowired
    private OperationTopMoviesByYear topMoviesByYear;
    @Test
    public void doWork() throws Exception {
        Dataset<Row> dataFrame = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/test/title.basics-test.csv");
        topMoviesByYear.setRatings_file("./data/test/title.ratings-test.csv");
        Dataset<Row> result = topMoviesByYear.doWork(dataFrame);

        String test_result = result.first().getAs("primaryTitle");
        Assert.assertEquals("Answer",test_result);
    }

}