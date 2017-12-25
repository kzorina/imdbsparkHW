package imdb_spark.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


/**
 * Created by zorka_000 on 07.12.2017.
 */

@Configuration
@ComponentScan(basePackages = "imdb_spark")
public class Conf {

    @Autowired
    private SparkConf sparkConf;

    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(sparkConf);
    }
    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }
}
