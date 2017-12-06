/**
 * Created by zorka_000 on 04.12.2017.
 */
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

public class Main
{
    private static String yourAccessKey = "AKIAJ32FGOW46ARWMULQ";
    private static String yourSecretKey = "fcTdxFOaxZYe6vZcgNenU3mCSaBTfo9uZ3dxGbX8";
    private static String bucketName = "imdb-datasets";
    private static String key        = "documents/v1/current/title.basics.tsv.gz";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("IMDB");
        sparkConf.setMaster("local[*]");//* - number of cores. here we don't care
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // try to load IMDB data to rdd

        sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", yourAccessKey);
        sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", yourSecretKey);// can contain "/"
        //sc.hadoopConfiguration().set("fs");

        JavaRDD<String> rdd_title_bas = sc.textFile("s3n://"+bucketName+"/documents/v1/current/title.basics.tsv.gz");

        //JavaRDD<String> rdd_title_bas = sc.textFile("title.basics.tsv");
        rdd_title_bas.persist(MEMORY_AND_DISK());
        long numberOfMovies = rdd_title_bas.count();
        System.out.println("numberOfMovies = "+numberOfMovies);//numberOfMovies = 4668775
        JavaRDD<Movie> movieRdd = rdd_title_bas.map(line->line.toLowerCase()).map(line->{
            String[] data = line.split("\t");
            //System.out.println("HERE IS SOME DATA");
            //System.out.println(data[0]);
            int movie_year = 0;
            try{
                movie_year = Integer.parseInt(data[5]);
            } catch(NumberFormatException e){}
            Movie movie = Movie.builder().type(data[1]).title(data[2]).isAdult(Boolean.parseBoolean(data[4])).
                        year(movie_year).time(data[7]).genres(new String[]{data[8]}).build();
            return movie;
        });
        movieRdd.persist(MEMORY_AND_DISK());
        System.out.println("Starting to filter");
//        JavaPairRDD<String, Integer> tuples = movieRdd.mapToPair(movie -> new Tuple2<>(movie.getTitle(),movie.getYear())); // 16 video 12 min
        JavaRDD<Movie> movies1980 = movieRdd.filter(movie -> movie.getYear()==1980);
        movies1980.persist(MEMORY_AND_DISK());
        long numberOf1980Movies = movies1980.count();
        System.out.println("Movies in 1980 : "+numberOf1980Movies);//Movies in 1980 : 22104

    }
}
