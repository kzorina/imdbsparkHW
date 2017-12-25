package old_shit;
import old_shit.RddCreators.*;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by zorka_000 on 06.12.2017.
 */
public interface MovieCounter {
    long movieCounter(JavaRDD<Movie> movie);
}
