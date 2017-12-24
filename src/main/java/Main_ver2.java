/**
 * Created by zorka_000 on 04.12.2017.
 */
import RddCreators.Movie;
import RddCreators.Rating;
import RddCreators.ShowDataframeInTheEnd;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Profile;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;
@Profile(Const.DEV)
public class Main_ver2
{

//    private static String yourAccessKey = "AKIAJ32FGOW46ARWMULQ";
//    private static String yourSecretKey = "fcTdxFOaxZYe6vZcgNenU3mCSaBTfo9uZ3dxGbX8";
//    private static String bucketName = "imdb-datasets";
//    private static String key        = "documents/v1/current/title.basics.csv.gz";
    @ShowDataframeInTheBeginning
    @ShowDataframeInTheEnd
    public static Dataset<Row> findBestOf1980(Dataset<Row> dataFrame1,Dataset<Row> dataFrame2){


        return dataFrame1.filter("startYear=1980").
                filter(dataFrame1.col("titleType").equalTo("movie")).join(dataFrame2,dataFrame1.col("tconst").equalTo(dataFrame2.col("tconst"))).filter(dataFrame2.col("numVotes").gt(100)).orderBy(dataFrame2.col("averageRating").desc());
    }


    public static void main(String[] args) {
        // AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);

        //ShowDFAspect showAspect = context.getBean(ShowDFAspect.class);
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("IMDB");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //SparkSession spark = SparkSession.builder().appName("Dataframes get started").getOrCreate();
        SQLContext sqlcon = new SQLContext(sc);

        //Dataset<Row> df = spark.read().format("tsv").option("header","true").load("title.basics.csv");
        Dataset<Row> df_basic = sqlcon.read().format("csv").option("header", "true").option("delimiter","\t").load("title.basics.csv");
        Dataset<Row> df_rating = sqlcon.read().format("csv").option("header", "true").option("delimiter","\t").load("title.ratings.csv");
        df_basic.printSchema();
        Dataset<Row> bestOf1980 = findBestOf1980(df_basic,df_rating);
        bestOf1980.show();

//        sparkConf.setAppName("IMDB");
//        sparkConf.setMaster("local[*]");//* - number of cores. here we don't care
//
//        // try to load IMDB data to rdd from aws
////
////        sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", yourAccessKey);
////        sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", yourSecretKey);// can contain "/"
////        //sc.hadoopConfiguration().set("fs");
////
////        JavaRDD<String> rdd_title_bas = sc.textFile("s3n://"+bucketName+"/documents/v1/current/title.basics.csv.gz");
////
//        JavaRDD<String> rdd_title_bas = sc.textFile("title.basics.csv");
//        rdd_title_bas.persist(MEMORY_AND_DISK());
//       int kkk = 0;
//        JavaPairRDD<String,Movie> movieRdd = rdd_title_bas.map(line->line.toLowerCase()).mapToPair(line->{
//            String[] data = line.split("\t");
//
//            //System.out.println(data[0]);
//
//            //System.out.println("HERE IS SOME DATA");
//            //System.out.println(data[0]);
//            int movie_year = 0;
//            try{
//                movie_year = Integer.parseInt(data[5]);
//            } catch(NumberFormatException e){}
//            Movie movie = Movie.builder().type(data[1]).title(data[2]).isAdult(Boolean.parseBoolean(data[4])).
//                        year(movie_year).time(data[7]).genres(new String[]{data[8]}).build();
//            return new Tuple2(data[0],movie);
//        });
//        movieRdd.persist(MEMORY_AND_DISK());
//        long numberOfMovies = movieRdd.count();
//        System.out.println("numberOfMovies = "+numberOfMovies);//numberOfMovies = 4668775
//        JavaRDD<String> rdd_rating = sc.textFile("title.ratings.csv");
//        rdd_rating.persist(MEMORY_AND_DISK());
////        long numberOfMovies = rdd_rating.count();
////        System.out.println("numberOfMovies = "+numberOfMovies);//numberOfMovies = 4668775
//        JavaPairRDD<String,Rating> ratingRdd = rdd_rating.map(line->line.toLowerCase()).mapToPair(line->{
//            String[] data = line.split("\t");
//            //System.out.println("HERE IS SOME DATA");
//            //System.out.println(data[0]);
//            Double avRating = 0.0;
//            long numOfVotes = 0;
//            try{
//                avRating = Double.parseDouble(d ata[1]);
//                numOfVotes = Long.parseLong(data[2]);
//            } catch(NumberFormatException e){}
//            Rating rating = Rating.builder().avRating(avRating).numOfVotes(numOfVotes).build();
//            return new Tuple2(data[0],rating);
//        });
//        long numberOfRatings = ratingRdd.count();
//        System.out.println("numberOfRatings = "+numberOfRatings);
//
//        //System.out.println("Starting to filter");
////        JavaPairRDD<String, Integer> tuples = movieRdd.mapToPair(movie -> new Tuple2<>(movie.getTitle(),movie.getYear())); // 16 video 12 min
//        Function<Tuple2<String, Movie>, Boolean> myFilter =
//                new Function<Tuple2<String, Movie>, Boolean>() {
//                    public Boolean call(Tuple2<String, Movie> keyValue) {
//                        if (keyValue._2().getYear() ==1980){
//                            //System.out.println(keyValue._2().getYear());
//                        }
//                        return (keyValue._2().getYear() ==1980);
//                    }
//                };
//        JavaPairRDD<String,Movie> movies1980 = movieRdd.filter(myFilter);
//        long numberOfMovies1980 = movies1980.count();
//        System.out.println("numberOfMovies1980filter = "+numberOfMovies1980);
//        movies1980.persist(MEMORY_AND_DISK());
//        JavaPairRDD<String,Tuple2<Movie,Rating>> movies1980Rating = movies1980.join(ratingRdd);
//        long numberOfMovies1980rate = movies1980Rating.count();
//        System.out.println("numberOfMovies1980rate = "+numberOfMovies1980rate);
//        //TupleComparator comparator = new TupleComparator();
//        List<Tuple2<Movie,Rating>> answer =
//                movies1980Rating.mapToPair(Tuple2::swap).sortByKey(new TupleComparator(),false).map(Tuple2::_1).take(5);
//        int counter = 0;
//        System.out.println("list size: "+answer.size());
//
//        for (Tuple2<Movie,Rating> t : answer){
//            if (counter<10) {
//                counter++;
//                System.out.println("Movie : " + t._1().getTitle() + " with rating " + t._2().getAvRating());
//            }
//        }
//        //long numberOf1980Movies = movies1980.count();
//        //System.out.println("Movies in 1980 : "+numberOf1980Movies);//Movies in 1980 : 22104
//
//    }
//    public static class TupleComparator implements Comparator<Tuple2<Movie,Rating>>,Serializable
//    {
//        public int compare (Tuple2 < Movie, Rating > tuple1, Tuple2 < Movie, Rating > tuple2){
//        Double res = (tuple1._2().getAvRating() - tuple2._2().getAvRating()) * 100;
//        return res.intValue();
//    }
//    }
    }
}

