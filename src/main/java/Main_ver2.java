/**
 * Created by zorka_000 on 04.12.2017.
 */

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main_ver2 {
     public static void main(String[] args) {
        System.setProperty("spring.profiles.active", Const.DEV);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);

        BusinessLogic businessLogic = context.getBean(BusinessLogic.class);
        businessLogic.doWork();
    }
}

