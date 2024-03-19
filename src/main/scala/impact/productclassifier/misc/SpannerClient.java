package impact.productclassifier.misc;

import com.google.cloud.spanner.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SpannerClient {

    private final DatabaseClient dbClient;
    
    private static final List<String> COLUMNS = Arrays.asList(
            "CatalogId", "CatalogItemId", "Category", "Name", "Description", "Manufacturer", "Labels",
            "IsParent", "ParentName", "ParentSku", "Attributes"
    );
    
    public SpannerClient() {
        try {
            PdsGoogleCredentials pdsGoogleCredentials = new PdsGoogleCredentials("");
            SpannerOptions options = SpannerOptions.newBuilder()
                    .setCredentials(pdsGoogleCredentials.getCredentialsProvider().getCredentials())
                    .setAutoThrottleAdministrativeRequests()
                    .build();
            Spanner spanner = options.getService();
            DatabaseId db = DatabaseId.of("rad-prod-pds", "pds-production", "pds-prod");
            dbClient = spanner.getDatabaseClient(db);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Product> readProductData(long catalogId, String lastSku, int pageSize) {
        String query = "SELECT "
                + "CatalogId, CatalogItemId, "
                + "JSON_VALUE(PARSE_JSON(Attributes, wide_number_mode=>'round'), \"$.taxonomy.filterCategory\") AS Category, "
                + "Name, Description, Manufacturer, Labels, IsParent, ParentName, ParentSku, Attributes\n"
                + "FROM ProductData\n"
                + "WHERE CatalogId = @catalog_id "
                + "AND CatalogItemId > @last_sku "
                + "AND Name IS NOT NULL "
                + "AND Attributes LIKE \"%filterCategory%\"\n"
                + "ORDER BY CatalogItemId\n"
                + "LIMIT @page_size";
        Statement statement = Statement.newBuilder(query)
                .bind("catalog_id").to(catalogId)
                .bind("last_sku").to(lastSku)
                .bind("page_size").to(pageSize)
                .build();
        try (ReadContext context = dbClient.singleUse(); ResultSet resultSet = context.executeQuery(statement)) {
            return getProducts(resultSet);
        }
    }

    public List<Product> readWalmartProductData(
            long catalogId,
            String lastSku,
            int pageSize,
            List<String> excludedCategories,
            List<String> filterCategories) {
        String exclusions = excludedCategories.stream()
                .map(cat -> " AND LOWER(Category) NOT LIKE \"" + cat + "\" ")
                .collect(Collectors.joining());
        String filters = filterCategories.stream()
                .map(cat -> " AND LOWER(Category) LIKE \"" + cat + "\" ")
                .collect(Collectors.joining());
        String query = "SELECT "
                + "CatalogId, CatalogItemId, Category, "
                + "Name, Description, Manufacturer, Labels, IsParent, ParentName, ParentSku, Attributes\n"
                + "FROM ProductData\n"
                + "WHERE CatalogId = @catalog_id "
                + "AND CatalogItemId > @last_sku "
                + "AND Name IS NOT NULL "
                + "AND Category IS NOT NULL AND Category <> \"UNNAV\" "
                + exclusions
                + filters
                + "ORDER BY CatalogItemId\n"
                + "LIMIT @page_size";
        Statement statement = Statement.newBuilder(query)
                .bind("catalog_id").to(catalogId)
                .bind("last_sku").to(lastSku)
                .bind("page_size").to(pageSize)
                .build();
        try (ReadContext context = dbClient.singleUse(); ResultSet resultSet = context.executeQuery(statement)) {
            return getProducts(resultSet);
        }
    }

    public List<Product> readWalmartProductData(KeySet keySet) {
        try (ReadContext context = dbClient.singleUse(); ResultSet resultSet = context.read("ProductData", keySet, COLUMNS)) {
            return getProducts(resultSet);
        }
    }
    
    private List<Product> getProducts(ResultSet resultSet) {
        Instant start = Instant.now();
        List<Product> products = new ArrayList<>();
        while (resultSet.next()) {
            Product product = Product.mapFields(resultSet);
            if (product.getGmcCategory() != null) {
                products.add(product);
            }
        }
        long millis = Duration.between(start, Instant.now()).toMillis(); 
        System.out.printf("Retrieved %d products in %d millis%n", products.size(), millis);
        return products;
    }
    
    
}
