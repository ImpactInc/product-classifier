/*
 *
 *  * Copyright (C) 2016 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *  
 */

package impact.productclassifier.misc;

import lombok.*;

import java.util.Set;


/**
 * Created by stephen on 9/9/16.
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class FinancialAttributes {

    public enum ProductType {
        Business, Consumer, Student
    }

    public enum CreditRating {
        Excellent, Good, Fair, Average, Poor, Bad, LimitedHistory, NoHistory, Limited, NoCredit
    }

    public enum Application {
        Desktop, Mobile, Phone
    }

    public enum RewardsType {
        Miles, Points, Cashback, Starpoints
    }

    @AllArgsConstructor
    @Getter
    public enum PeriodType {
        MONTHS("Months"), DAYS("Days"), YEARS("Years");

        private final String value;
    }
    
    private ProductType productType;
    private Set<CreditRating> creditRating;
    private PeriodType introPurchaseAprPeriodType;
    private PeriodType introTransferAprPeriodType;
    private RewardsType rewardsType;
    private RewardsType rewardsType2;
    private RewardsType rewardsType3;
    private Set<Application> availableApplications;
}
