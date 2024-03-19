package impact.productclassifier.misc;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static impact.productclassifier.misc.CsvUtil.getCsvFormat;
import static impact.productclassifier.misc.CsvUtil.getFilePath;


public class TrainingDataDownloader {

    private static final int PAGE_SIZE = 7500;
    
    private static final List<String> EXCLUDED_WALMART_CATEGORIES = Arrays.asList();

    private static final List<String> FILTER_WALMART_CATEGORIES = Arrays.asList(
            "pets>%"
    );

    private final SpannerClient spannerClient = new SpannerClient();

    public void refreshTrainingData() {
        String dir = "Documents/PDS/Taxonomy/training-data";
        List<File> files = Arrays.asList(getFilePath(dir).toFile().listFiles());
        List<Long> catalogs = files.stream()
                .filter(file -> file.getName().endsWith(".csv"))
                .map(file -> Long.parseLong(file.getName().split("\\.")[0]))
                .collect(Collectors.toList());
        catalogs.forEach(this::downloadCatalog);
    }
    
    public void downloadTrainingData() {
        downloadCatalog(2373);
        downloadCatalog(9250);
        downloadCatalog(2115);
        downloadCatalog(5685);
        downloadCatalog(8801);
        downloadCatalog(8921);
        downloadCatalog(8922);
        downloadCatalog(2285);
        downloadCatalog(8073);
        downloadCatalog(9362);
        downloadCatalog(9474);
        downloadCatalog(9555);
        downloadCatalog(5118);
    }

    public void downloadWalmartData() {
        downloadWalmartCatalog(9743);
    }

    public void refreshWalmartData() {
        String dir = "Documents/PDS/Taxonomy/training-data/walmart-catalogs";
        List<File> files = Arrays.asList(getFilePath(dir).toFile().listFiles());
        List<Long> catalogs = files.stream()
                .filter(file -> file.getName().endsWith(".csv"))
                .map(file -> Long.parseLong(file.getName().split("\\.")[0]))
                .collect(Collectors.toList());
        catalogs.forEach(this::refreshWalmartCatalog);
    }

    private void downloadCatalog(long catalogId) {
        System.out.println("Downloading catalog " + catalogId);
        String lastSku = "";
        Instant start = Instant.now();
        int numRecords = 0;
        Path filePath = getFilePath(String.format("Documents/PDS/Taxonomy/training-data/%d.csv", catalogId));
        try (BufferedWriter output = Files.newBufferedWriter(filePath);
             CSVPrinter csvPrinter = new CSVPrinter(output, getCsvFormat(';'))) {
            csvPrinter.printRecord(Product.FIELDS);

            List<Product> products = spannerClient.readProductData(catalogId, lastSku, PAGE_SIZE);
            while (!products.isEmpty()) {
                numRecords += products.size();
                for (Product product : products) {
                    csvPrinter.printRecord(product.getValues());
                }
                lastSku = products.get(products.size() - 1).getCatalogItemId();
                products = spannerClient.readProductData(catalogId, lastSku, PAGE_SIZE);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long seconds = Duration.between(start, Instant.now()).getSeconds();
        System.out.printf("Downloaded %d products in %d seconds%n", numRecords, seconds);
    }

    private void downloadWalmartCatalog(long catalogId) {
        System.out.println("Downloading catalog " + catalogId);
        String lastSku = "";
        Instant start = Instant.now();
        int numRecords = 0;
        Path filePath = getFilePath(String.format("Documents/PDS/Taxonomy/training-data/walmart-catalogs/%d.csv", catalogId));
        try (BufferedWriter output = Files.newBufferedWriter(filePath);
             CSVPrinter csvPrinter = new CSVPrinter(output, getCsvFormat(';'))) {
            csvPrinter.printRecord(Product.FIELDS);

            List<Product> products = spannerClient.readWalmartProductData(catalogId,
                    lastSku,
                    PAGE_SIZE,
                    EXCLUDED_WALMART_CATEGORIES,
                    FILTER_WALMART_CATEGORIES);
            while (!products.isEmpty()) {
                numRecords += products.size();
                for (Product product : products) {
                    csvPrinter.printRecord(product.getValues());
                }
                lastSku = products.get(products.size() - 1).getCatalogItemId();
                products = spannerClient.readWalmartProductData(catalogId,
                        lastSku,
                        PAGE_SIZE,
                        EXCLUDED_WALMART_CATEGORIES,
                        FILTER_WALMART_CATEGORIES);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long seconds = Duration.between(start, Instant.now()).getSeconds();
        System.out.printf("Downloaded %d products in %d seconds%n", numRecords, seconds);
    }

    private void refreshWalmartCatalog(long catalogId) {
        System.out.println("Refreshing catalog " + catalogId);
        Instant start = Instant.now();
        int numRecords = 0;
        Path filePath = getFilePath(String.format("Documents/PDS/Taxonomy/training-data/walmart-catalogs/%d.csv", catalogId));

        // Get row keys
        List<String> catalogItemIds = new ArrayList<>();
        String column = Product.Field.catalogItemId.name();
        try (CSVParser parser = CSVParser.parse(filePath, StandardCharsets.UTF_8, getCsvFormat(';'))) {
            parser.forEach(csvRecord -> catalogItemIds.add(csvRecord.get(column)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (BufferedWriter output = Files.newBufferedWriter(filePath);
             CSVPrinter csvPrinter = new CSVPrinter(output, getCsvFormat(';'))) {
            csvPrinter.printRecord(Product.FIELDS);
            
            int i = 0;
            while (i < catalogItemIds.size()) {
                KeySet.Builder keySetBuilder = KeySet.newBuilder();
                int end = Math.min(i + 5000, catalogItemIds.size());
                catalogItemIds.subList(i, end).forEach(sku -> keySetBuilder.addKey(Key.of(catalogId, sku)));
                List<Product> products = spannerClient.readWalmartProductData(keySetBuilder.build());
                numRecords += products.size();
                for (Product product : products) {
                    csvPrinter.printRecord(product.getValues());
                }
                i = end;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long seconds = Duration.between(start, Instant.now()).getSeconds();
        System.out.printf("Downloaded %d products in %d seconds%n", numRecords, seconds);
    }
}
