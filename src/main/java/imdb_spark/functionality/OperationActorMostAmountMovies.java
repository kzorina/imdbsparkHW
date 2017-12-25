package imdb_spark.functionality;
import imdb_spark.custom_annotations.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.explode;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Operation
public class OperationActorMostAmountMovies implements OperationInterface {
    String description = "Find actor with higher amount of movies";
    @Autowired
    private SQLContext sqlContext;
    @Override
    public String getDescription(){
        return this.description;
    }
    @Override
    public Dataset<Row> doWork(Dataset<Row> dataFrame) {

        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/title.ratings.csv");
        Dataset<Row> df_names = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/name.basics.csv");

        Dataset<Row> df_principals = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/title.principals.csv");
        df_principals = df_principals.withColumn("arrayCast", functions.split(df_principals.col("principalCast"),",").cast("array<String>"));
        df_principals = df_principals.withColumn("nconst", explode(df_principals.col("arrayCast")));


        dataFrame = dataFrame.join(df_principals,dataFrame.col("tconst").equalTo(df_principals.col("tconst")))
                .join(df_names,df_principals.col("nconst").equalTo(df_names.col("nconst")));
        dataFrame = dataFrame.groupBy("primaryName").agg(functions.count("primaryTitle"));

        return dataFrame.
                orderBy(dataFrame.col("count(primaryTitle)").desc());
    }
}
