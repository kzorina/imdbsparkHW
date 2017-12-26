package imdb_spark.functionality;


import imdb_spark.Const;
import imdb_spark.configuration.Conf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import static org.junit.Assert.*;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Conf.class)
@ActiveProfiles(Const.DEV)
public class OperationActorMostAmountMoviesTest {
    @Autowired
    private SQLContext sqlContext;
    @Autowired
    private OperationActorMostAmountMovies mostMoviesActor;
    @Test
    public void doWork() throws Exception {
        Dataset<Row> dataFrame = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/test/title.basics-test.csv");
        mostMoviesActor.setName_file("./data/test/name.basics-test.csv");
        mostMoviesActor.setPrincipals_file("./data/test/title.principals-test.csv");
        Dataset<Row> result = mostMoviesActor.doWork(dataFrame);
        System.out.println(result.first());
        String test_result = result.first().getAs("primaryName");
        Assert.assertEquals("Vaynona Rayder",test_result);
    }

}