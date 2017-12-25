import imdb_spark.configuration.Conf;
import imdb_spark.Const;
import old_shit.RddCreators.Movie;
import old_shit.MovieCounter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zorka_000 on 07.12.2017.
 */

//@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = imdb_spark.configuration.Conf.class)
//@ActiveProfiles(imdb_spark.Const.DEV)

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Conf.class)
@ActiveProfiles(Const.DEV)
public class MovieCounterImplTest {
    @Autowired
    private MovieCounter movieCounter;
    @Autowired
    private JavaSparkContext sc;
    @Test
    public void movieCounter() throws Exception {
        String[] genres = {"horror", "drama"};
        List<Movie> movies = Arrays.asList(Movie.builder().id("tt1").type("short").title("first").isAdult(true).year(1990).time("45").genres(genres).build(),
                Movie.builder().id("tt2").type("short").title("second").isAdult(true).year(1980).time("47").genres(genres).build());

                //new Movie("tt1","short","second",true,1990,"45",genres),new Movie("tt2","short","first",true,1990,"45",genres));
        JavaRDD<Movie> rdd = sc.parallelize(movies);
        long numberOfMovies = movieCounter.movieCounter(rdd);
        Assert.assertEquals(2,numberOfMovies);
    }

}