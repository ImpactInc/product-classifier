/*
 *
 *  * Copyright (C) 2015 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *
 */

package impact.productclassifier.misc;

/**
 * Created by mihlali on 2015/04/16.
 */
public enum EnergyEfficiencyClass {
    
    classG("G"),
    classF("F"),
    classE("E"),
    classD("D"),
    classC("C"),
    classB("B"),
    classA("A"),
    classA_PLUS("A+"),
    classA_PLUS_PLUS("A++"),
    classA_PLUS_PLUS_PLUS("A+++");
    
    private final String energyEfficiencyClass;
    
    EnergyEfficiencyClass(String energyEfficiencyClass) {
        
        this.energyEfficiencyClass = energyEfficiencyClass;
    }
    
    @Override
    public String toString() {
        return energyEfficiencyClass;
    }
    
}
