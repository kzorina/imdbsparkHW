import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by zorka_000 on 05.12.2017.
 */
@Data
@Builder
public class Movie implements Serializable{
    private String type;
    private String title;
    private boolean isAdult;
    private int year;
    private String time;
    private String[] genres;

}
