/*
 *
 *  * Copyright (C) 2015 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *
 */

package impact.productclassifier.misc;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class TargetDemographics {

    public enum Gender {
        Male, Female, Unisex
    }

    public enum AgeGroup {
        Newborn, Infant, Toddler, Kids, Adult
    }
    
    private Gender gender;
    private AgeRange ageRange;
    private AgeGroup ageGroup;
    private Boolean adult;
}
