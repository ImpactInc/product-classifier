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

import java.math.BigDecimal;
import java.util.Currency;


/**
 * Created by stephen on 2015/04/20.
 */
@Getter
@Setter
@EqualsAndHashCode
public class Pricing {

    private Currency currency;
    private BigDecimal currentPrice; // Same as "buy it now" price
    private BigDecimal originalPrice; // The original price if current price indicates a sale price
    private BigDecimal dollarPrice; // currentPrice to dollar price
    private Integer discountPercentage;

    private SubscriptionCost subscriptionCost;
    
    // String representation of the price per unit e.g. "$2 / Lb."
    private String unitPrice;
    // EU and Switzerland only.
    // Unit pricing example: if price is 3 EUR, 'unit pricing measure' is 150ml, and 'unit pricing base measure' is 100ml the unit price would be 2 EUR / 100ml
    private UnitOfMeasure unitOfMeasure; // e.g. g
    private Integer baseMeasure; // e.g. 100
    private Integer includedMeasure; // e.g. 225

    private BigDecimal cost;
    
    public void setCurrentPrice(BigDecimal currentPrice) {
        this.currentPrice = currentPrice;
        updateDiscountPercentage();
    }
    
    public void setOriginalPrice(BigDecimal originalPrice) {
        this.originalPrice = originalPrice;
        updateDiscountPercentage();
    }
    
    public void setDiscountPercentage(Integer discountPercentage) {
        //Do nothing
    }
    
    private void updateDiscountPercentage() {
        final BigDecimal zeroPrice = BigDecimal.valueOf(0.0);
        if (originalPrice == null || currentPrice == null
                || currentPrice.compareTo(originalPrice) > 0
                || currentPrice.compareTo(zeroPrice) <= 0) {
            discountPercentage = null;
        } else {
            final BigDecimal priceDifference_x100 = originalPrice.subtract(currentPrice).movePointRight(2);
            final BigDecimal percentage = priceDifference_x100.divideToIntegralValue(originalPrice);
            final int percentageInt = percentage.intValue();
            discountPercentage = percentageInt > 0 ? percentageInt : null;
        }
    }
}
