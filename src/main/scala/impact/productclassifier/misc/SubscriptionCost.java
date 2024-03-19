package impact.productclassifier.misc;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;


/**
 * Created by mihlalintoni on 2021-09-03.
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class SubscriptionCost {

    public enum Period {
        MONTH, YEAR
    }
    
    private Period period;
    private int periodLength;
    private BigDecimal amount;
}
