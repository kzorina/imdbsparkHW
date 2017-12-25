package imdb_spark.configuration;

import imdb_spark.Const;
import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Created by zorka_000 on 07.12.2017.
 */

@Profile(Const.DEV)
@Configuration
public class DevConfig {
    @Bean
    public SparkConf sparkConf(){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("IMDB");
        sparkConf.setMaster("local[*]");
        return sparkConf;
    }
}
