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


/**
 * Created by stephen on 2015/04/20.
 */
@Getter
@Setter
@EqualsAndHashCode
public class Shipping {
    
    private Double weight;
    private WeightUnit weightUnit;

    private Double width;
    private Double length;
    private Double height;
    private LengthUnit lengthUnit;

    private String label;
    private Long zipCode;
}
