package imdb_spark;

import org.apache.spark.sql.Column;
import java.util.HashMap;
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
	static final HashMap<String,String> FILES_URLS = new HashMap<String,String>(){
		{
			put("name.basics","https://datasets.imdbws.com/name.basics.tsv.gz");
			put("title.basics","https://datasets.imdbws.com/title.basics.tsv.gz");
			put("title.principals","https://datasets.imdbws.com/title.principals.tsv.gz");
			put("title.ratings","https://datasets.imdbws.com/title.ratings.tsv.gz");
		}			
	};
}
