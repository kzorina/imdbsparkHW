package imdb_spark;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;

public interface Const {
    String DEV = "Dev";
    String PROD = "Prod";
    Column[] important_columns = {
            col("tconst"),
            col("primaryTitle"),
            col("startYear"),
            col("runtimeMinutes"),
            col("genres")};
    int numberOfVotesLimit = 1000;
}
