import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Profile(Const.DEV)
public class BusinessLogic {

    @Autowired
    private JavaSparkContext sc;
    @ShowDataframeInTheBeginning
    @ShowDataframeInTheEnd
    public static Dataset<Row> findBestOf1980(Dataset<Row> dataFrame1,Dataset<Row> dataFrame2){


        return dataFrame1.filter("startYear=1980").
                filter(dataFrame1.col("titleType").equalTo("movie")).join(dataFrame2,dataFrame1.col("tconst").equalTo(dataFrame2.col("tconst"))).filter(dataFrame2.col("numVotes").gt(100)).orderBy(dataFrame2.col("averageRating").desc());
    }
    public void doWork() {
        SQLContext sqlcon = new SQLContext(sc);

        //Dataset<Row> df = spark.read().format("tsv").option("header","true").load("title.basics.csv");
        Dataset<Row> df_basic = sqlcon.read().format("csv").option("header", "true").option("delimiter","\t").load("title.basics.csv");
        Dataset<Row> df_rating = sqlcon.read().format("csv").option("header", "true").option("delimiter","\t").load("title.ratings.csv");
        df_basic.printSchema();
        Dataset<Row> bestOf1980 = findBestOf1980(df_basic,df_rating);
        bestOf1980.show();
    }
}
