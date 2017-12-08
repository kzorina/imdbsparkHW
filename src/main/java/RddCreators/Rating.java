package RddCreators;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by zorka_000 on 05.12.2017.
 */
@Data
@Builder
public class Rating implements Serializable{
    private String id;
    private Double avRating;
    private long numOfVotes;
}
