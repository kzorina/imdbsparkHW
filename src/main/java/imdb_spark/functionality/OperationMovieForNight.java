package imdb_spark.functionality;
import imdb_spark.Const;
import imdb_spark.GetObject;
import imdb_spark.custom_annotations.*;
import lombok.Setter;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Console;
import java.io.File;
import java.util.ArrayList;
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

    @Setter
    public String year_from = "1980";
    @Setter
    public String year_to = "2000";
    @Setter
    public String genre = "Comedy";
    @Setter
    String ratings_file = "title.ratings";
    @Autowired
    private SQLContext sqlContext;
    @Override
    public ArrayList<String> requiredParameters(){
        ArrayList<String> list = new ArrayList<String>();
        list.add("year_from");
        list.add("year_to");
        list.add("genre");
        return list;
    }
    @Override
    public String getDescription(){
        return this.description;
    }
    @Override
    public Dataset<Row> doWork(Dataset<Row> dataFrame) {
        File f = new File("./data/".concat(ratings_file.concat(".csv")));
        if (!f.exists()){
            try {
                GetObject.downloadFile(ratings_file);
            }catch (Exception e){
                System.out.println(e);
            }
        }

        ratings_file = "./data/".concat(ratings_file.concat(".csv"));
        dataFrame = dataFrame.filter("startYear>="+year_from).filter("startYear<="+year_to).withColumn("genresArray", functions.split(dataFrame.col("genres"),",").cast("array<String>"));
        dataFrame = dataFrame.filter(array_contains(dataFrame.col("genresArray"),genre));

        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load(ratings_file);

        Column[] important_columns = {
                col("primaryTitle"),
                col("startYear"),
                col("runtimeMinutes"),
                col("genres"),
                col("averageRating")
        };
        dataFrame = dataFrame
                .join(df_rating,dataFrame.col("tconst").equalTo(df_rating.col("tconst")));
        dataFrame = dataFrame.filter(dataFrame.col("numVotes").gt(Const.numberOfVotesLimit));

        return dataFrame.orderBy(dataFrame.col("averageRating").desc()).select(important_columns);
    }
}
