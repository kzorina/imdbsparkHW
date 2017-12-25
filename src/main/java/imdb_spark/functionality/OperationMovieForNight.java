package imdb_spark.functionality;
import imdb_spark.custom_annotations.*;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Console;
import java.util.HashMap;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Operation
public class OperationMovieForNight implements OperationInterface {
    String description = "Find movie to watch depending on genre and year";
    @Autowired
    private SQLContext sqlContext;
    @Override
    public String getDescription(){
        return this.description;
    }
    @Override
    public Dataset<Row> doWork(Dataset<Row> dataFrame) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter operation code : ");
        int operation_code = Integer.parseInt(sc.nextLine());
        System.out.println("You entered : " + operation_code);
        int year_from = 1980;
        int year_to = 2000;
        String genre = "Comedy";

        dataFrame = dataFrame.filter("startYear>="+year_from).filter("startYear<="+year_to).withColumn("genresArray", functions.split(dataFrame.col("genres"),",").cast("array<String>"));
        dataFrame = dataFrame.filter(array_contains(dataFrame.col("genresArray"),genre));

        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/title.ratings.csv");

        Column[] important_columns = {
                col("primaryTitle"),
                col("startYear"),
                col("runtimeMinutes"),
                col("genres"),
                col("averageRating")
        };
        dataFrame = dataFrame
                .join(df_rating,dataFrame.col("tconst").equalTo(df_rating.col("tconst")));
        dataFrame = dataFrame.filter(dataFrame.col("numVotes").gt(1000));

        return dataFrame.orderBy(dataFrame.col("averageRating").desc()).select(important_columns);
    }
}
