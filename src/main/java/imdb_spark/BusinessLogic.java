package imdb_spark;
import static org.apache.spark.sql.functions.col;
import imdb_spark.custom_annotations.*;
import imdb_spark.functionality.OperationActorMostAmountMovies;
import imdb_spark.functionality.OperationActorTopSumRating;
import imdb_spark.functionality.OperationInterface;
import imdb_spark.functionality.OperationTopMoviesByYear;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.Console;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

/**
 * Created by zorka_000 on 25.12.2017.
 */
@Service
@Profile(Const.DEV)
public class BusinessLogic {
    private String includedTypes = "movie";
    @Autowired
    private SQLContext sqlContext;
    @Autowired
    private ApplicationContext context;
    @Autowired
    private JavaSparkContext sc;
    @Autowired
    private OperationTopMoviesByYear topByYear;
    @Autowired
    private OperationActorMostAmountMovies mostMoviesActor;
    @Autowired
    private OperationActorTopSumRating topActor;
    @Autowired
    private ShowDFAspect showDf;

    public void setParameters(OperationInterface workingOperation){
        for (String parameterName:workingOperation.requiredParameters()){
            Scanner sc = new Scanner(System.in);
            System.out.println("Please enter "+parameterName+" : ");
            String parameter = sc.nextLine();
            try{
                workingOperation.getClass().getDeclaredField(parameterName).set(workingOperation,parameter);
            }catch(Exception e){
                System.out.println(e);
            }

        }

    }

    public void mainLogicWork() {
        Dataset<Row> df_basic = sqlContext.read().format("csv").option("header", "true").option("delimiter","\t").load("./data/title.basics.csv");
        df_basic.persist(MEMORY_AND_DISK());
        df_basic = df_basic.filter("titleType='"+includedTypes+"'");
        df_basic = df_basic.select(Const.important_columns);


        Collection<Object> operations = context.getBeansWithAnnotation(Operation.class).values();
        HashMap<Integer,OperationInterface> possibleOperations = new HashMap<Integer,OperationInterface>();
        int counter = 0;
        for (Object operation : operations) {
            counter++;
            OperationInterface operationn = (OperationInterface) context.getBean(operation.getClass());
            System.out.println(counter+" - "+operationn.getDescription());
            possibleOperations.put(counter,operationn);

        }
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter operation code (among the above): ");
        int operation_code = Integer.parseInt(sc.nextLine());
        try{
            OperationInterface workingOperation = possibleOperations.get(operation_code);
            setParameters(workingOperation);
            Dataset<Row> result = workingOperation.doWork(df_basic);
            result.show();
        }catch (Exception e){
            System.out.println("You entered : " + operation_code+", and it is not valid code, sorry.");
        }

    }
}
