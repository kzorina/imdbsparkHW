import RddCreators.Movie;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * Created by zorka_000 on 06.12.2017.
 */
@Service
public class MovieCounterImpl implements MovieCounter, Serializable {

    @Override
    public long movieCounter(JavaRDD<Movie> movies) {
        return movies.count();
    }
}
