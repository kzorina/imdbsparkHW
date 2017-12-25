package imdb_spark.functionality;

import imdb_spark.custom_annotations.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Operation
public class OperationTopMoviesByYear implements OperationInterface {
    String description = "Find top movies for some year";
    private int numberOfVotesLimit = 1000;

    @Override
    public String getDescription(){
        return this.description;
    }
    @Autowired
    private SQLContext sqlContext;

    @ShowDataframeInTheBeginning
    @ShowDataframeInTheEnd
    @Override
    public Dataset<Row> doWork(Dataset<Row> dataFrame) {
        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/title.ratings.csv");
        String year = "1988";

        return dataFrame.join(df_rating,dataFrame.col("tconst").equalTo(df_rating.col("tconst"))).filter("startYear="+year).
               filter(df_rating.col("numVotes").gt(numberOfVotesLimit)).orderBy(df_rating.col("averageRating").desc());
    }
}
