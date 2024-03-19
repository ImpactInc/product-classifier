/*
 *
 *  * Copyright (C) 2015 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *
 */

package impact.productclassifier.misc;

import lombok.*;

import java.util.List;


/**
 * Created by stephen on 2015/04/20.
 */

@Builder
@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PhysicalAttributes {

    public enum ApparelSize {
        XS, S, M, l, XL
    }

    public enum ApparelSizeSystem {
        US, UK, EU, DE, FR, JP, CN, IT, BR, MEX, AU,
    }

    public enum ApparelSizeType {
        Regular, Petite, Plus, BigAndTall, Big, Tall, Maternity
    }

    public enum Condition {
        New, Used, Refurbished, OEM, OpenBox
    }
    
    // List of colors in order of prominence in multicolored products, eg Black/White for a black shirt with white stripes.
    // For products that come in different colors, they should be submitted as separate items with the same parentSku in Variant.
    private List<String> color;
    private String material;
    private String pattern;
    private ApparelSize apparelSize;
    private ApparelSizeSystem apparelSizeSystem;
    private ApparelSizeType apparelSizeType;
    // This is a String since it could be e.g. 33x16 for neck and sleeve measurements
    private String size;
    private SizeUnit sizeUnit;
    
    private Double productWeight;
    private WeightUnit weightUnit;
    
    private Double productHeight;
    private Double productWidth;
    private Double productLength;
    private LengthUnit lengthUnit;
    
    private Boolean bundle;
    
    private Condition condition;
    
    // EU and Switzerland only.
    private EnergyEfficiencyClass energyEfficiencyClass;

}
