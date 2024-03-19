/*
 *
 *  * Copyright (C) 2015 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *
 */

package impact.productclassifier.misc;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;


@EqualsAndHashCode
@Getter
@Setter
public class AgeRange {

    public enum AgeRangeUnit {
        Months, Years
    }
    
    private int ageRangeMin;
    private int ageRangeMax;
    private AgeRangeUnit ageRangeUnit;
    
    public AgeRange() {
    }
    
    public AgeRange(int ageRangeMin, int ageRangeMax, AgeRangeUnit ageRangeUnit) {
        this.ageRangeMin = ageRangeMin;
        this.ageRangeMax = ageRangeMax;
        this.ageRangeUnit = ageRangeUnit;
    }
}
