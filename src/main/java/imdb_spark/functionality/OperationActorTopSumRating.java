package imdb_spark.functionality;

import imdb_spark.Const;
import imdb_spark.custom_annotations.*;
import lombok.Setter;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Operation
public class OperationActorTopSumRating implements OperationInterface {
    String description = "Find actor with higher rating amoung all movie he casted in";

    @Setter
    String name_file = "./data/name.basics.csv";
    @Setter
    String principals_file = "./data/title.principals.csv";
    @Setter
    String ratings_file = "./data/title.ratings.csv";
    @Override
    public ArrayList<String> requiredParameters(){
        ArrayList<String> list = new ArrayList<String>();
        return list;
    }
    @Autowired
    private SQLContext sqlContext;
    @Override
    public String getDescription(){
        return this.description;
    }
    @Override
    public Dataset<Row> doWork(Dataset<Row> dataFrame) {
        Dataset<Row> df_rating = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load(ratings_file);
        Dataset<Row> df_names = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load(name_file);
        Dataset<Row> df_principals = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load(principals_file);

        df_principals = df_principals.withColumn("arrayCast",functions.split(df_principals.col("principalCast"),",").cast("array<String>"));
        df_principals = df_principals.withColumn("nconst", explode(df_principals.col("arrayCast")));

        dataFrame = dataFrame.join(df_principals,dataFrame.col("tconst").equalTo(df_principals.col("tconst")))
                .join(df_rating,dataFrame.col("tconst").equalTo(df_rating.col("tconst")))
                .join(df_names,df_principals.col("nconst").equalTo(df_names.col("nconst")));
        dataFrame = dataFrame.filter(dataFrame.col("numVotes").gt(Const.numberOfVotesLimit))
                .groupBy("primaryName").agg(avg("averageRating"));

        return dataFrame.orderBy(dataFrame.col("avg(averageRating)").desc());

    }
}
