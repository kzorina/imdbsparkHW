package imdb_spark.functionality;

import imdb_spark.Const;
import imdb_spark.custom_annotations.*;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.math.Ordering;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Operation
public class OperationTopMoviesByYear implements OperationInterface {
    String description = "Find top movies for some year";

    @Setter
    public String year = "1988";
    @Setter
    String ratings_file = "./data/title.ratings.csv";
    @Override
    public ArrayList<String> requiredParameters(){
        ArrayList<String> list = new ArrayList<String>();
        list.add("year");
        return list;
    }

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
        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load(ratings_file);

        return dataFrame.join(df_rating,dataFrame.col("tconst").equalTo(df_rating.col("tconst"))).filter("startYear="+year).
               filter(df_rating.col("numVotes").gt(Const.numberOfVotesLimit)).orderBy(df_rating.col("averageRating").desc());
    }
}
