package impact.productclassifier.misc;


import lombok.*;


@Data
@AllArgsConstructor
public class SpannerProductAttributes {
    private Pricing pricing;
    private Shipping shipping;
    private Taxonomy taxonomy;
    private PhysicalAttributes physicalAttributes;
    private TargetDemographics targetDemographics;
    private MediaAttributes mediaAttributes;
    private SoftwareAttributes softwareAttributes;
    private CellAttributes cellAttributes;
    private WatchAttributes watchAttributes;
    private BookAttributes bookAttributes;
    private FinancialAttributes financialAttributes;
}



