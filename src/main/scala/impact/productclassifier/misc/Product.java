package impact.productclassifier.misc;

import com.google.cloud.spanner.ResultSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Builder;
import lombok.Getter;

import java.util.Currency;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Builder
@Getter
public class Product {

    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Currency.class, new CurrencyAdapter()).create();

    enum Field {
        catalogId, catalogItemId, name, isParent, parentName, parentSku, manufacturer, labels, description,
        pricing, shipping, physicalAttributes, targetDemographics, mediaAttributes, softwareAttributes, cellAttributes, 
        watchAttributes, bookAttributes, financialAttributes,
        gmcCategory
    }

    static final List<String> FIELDS = Stream.of(Field.values()).map(Field::name).collect(Collectors.toList());

    private long catalogId;
    private String catalogItemId;
    private String gmcCategory;
    private String name;
    // Variant
    private boolean isParent;
    private String parentName;
    private String parentSku;
    private String manufacturer;
    private List<String> labels;
    private String description;

    private Pricing pricing;
    private Shipping shipping;
    private PhysicalAttributes physicalAttributes;
    private TargetDemographics targetDemographics;
    private MediaAttributes mediaAttributes;
    private SoftwareAttributes softwareAttributes;
    private CellAttributes cellAttributes;
    private WatchAttributes watchAttributes;
    private BookAttributes bookAttributes;
    private FinancialAttributes financialAttributes;

    public static Product mapFields(ResultSet resultSet) {
        String attributesString = getString(resultSet, "Attributes");
        SpannerProductAttributes attributes = GSON.fromJson(attributesString, SpannerProductAttributes.class);
        return Product.builder()
                .catalogId(resultSet.getLong("CatalogId"))
                .catalogItemId(getString(resultSet, "CatalogItemId"))
                .name(getString(resultSet, "Name"))
                .isParent(resultSet.getBoolean("IsParent"))
                .parentName(getString(resultSet, "ParentName"))
                .parentSku(getString(resultSet, "ParentSku"))
                .manufacturer(getString(resultSet, "Manufacturer"))
                .labels(getStringList(resultSet, "Labels"))
                .description(getString(resultSet, "Description"))
                .gmcCategory(getString(resultSet, "Category"))
                .pricing(attributes.getPricing())
                .shipping(attributes.getShipping())
                .physicalAttributes(attributes.getPhysicalAttributes())
                .targetDemographics(attributes.getTargetDemographics())
                .mediaAttributes(attributes.getMediaAttributes())
                .softwareAttributes(attributes.getSoftwareAttributes())
                .cellAttributes(attributes.getCellAttributes())
                .watchAttributes(attributes.getWatchAttributes())
                .bookAttributes(attributes.getBookAttributes())
                .financialAttributes(attributes.getFinancialAttributes())
                .build();
    }

    Object[] getValues() {
        return new Object[] {
                catalogId,
                catalogItemId,
                name,
                isParent,
                parentName,
                parentSku,
                manufacturer,
                labels,
                description,
                GSON.toJson(pricing),
                GSON.toJson(shipping),
                GSON.toJson(physicalAttributes),
                GSON.toJson(targetDemographics),
                GSON.toJson(mediaAttributes),
                GSON.toJson(softwareAttributes),
                GSON.toJson(cellAttributes),
                GSON.toJson(watchAttributes),
                GSON.toJson(bookAttributes),
                GSON.toJson(financialAttributes),
                gmcCategory
        };
    }

    private static String getString(ResultSet resultSet, String column) {
        return resultSet.isNull(column) ? null : resultSet.getString(column);
    }

    private static List<String> getStringList(ResultSet resultSet, String column) {
        return resultSet.isNull(column) ? null : resultSet.getStringList(column);
    }
}
