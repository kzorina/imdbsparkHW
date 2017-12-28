package imdb_spark.functionality;

import imdb_spark.Const;
import imdb_spark.GetObject;
import imdb_spark.custom_annotations.*;
import lombok.Setter;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.File;
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
    String name_file = "name.basics";
    @Setter
    String principals_file = "title.principals";
    @Setter
    String ratings_file = "title.ratings";
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
        File f1 = new File("./data/".concat(name_file.concat(".csv")));
        File f2 = new File("./data/".concat(principals_file.concat(".csv")));
        File f3 = new File("./data/".concat(ratings_file.concat(".csv")));
        if (!f1.exists()){
            try {
                GetObject.downloadFile(name_file);
            }catch (Exception e){
                System.out.println(e);
            }
        }
        if (!f2.exists()){
            try {
                GetObject.downloadFile(principals_file);
            }catch (Exception e){
                System.out.println(e);
            }
        }
        if (!f3.exists()){
            try {
                GetObject.downloadFile(ratings_file);
            }catch (Exception e){
                System.out.println(e);
            }
        }
        name_file = "./data/".concat(name_file.concat(".csv"));
        principals_file = "./data/".concat(principals_file.concat(".csv"));
        ratings_file = "./data/".concat(ratings_file.concat(".csv"));


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
