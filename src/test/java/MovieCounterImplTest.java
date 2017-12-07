import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Arrays;
import java.util.List;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import static org.junit.Assert.*;

/**
 * Created by zorka_000 on 07.12.2017.
 */

//@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = Conf.class)
//@ActiveProfiles(Const.DEV)

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
        List<Movie> movies = Arrays.asList(new Movie("short","second",true,1990,"45",genres),new Movie("short","first",true,1990,"45",genres));
        JavaRDD<Movie> rdd = sc.parallelize(movies);
        long numberOfMovies = movieCounter.movieCounter(rdd);
        Assert.assertEquals(2,numberOfMovies);
    }

}