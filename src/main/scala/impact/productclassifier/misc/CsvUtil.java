package impact.productclassifier.misc;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvUtil {
    public static CSVFormat getCsvFormat(char delimiter) {
        if (delimiter == ',') {
            return CSVFormat.DEFAULT.builder().setHeader().build();
        } else if (delimiter == '\t') {
            return CSVFormat.TDF.builder().setHeader().build();
        } else {
            return CSVFormat.DEFAULT.builder().setHeader().setDelimiter(delimiter).build();
        }
    }
    public static Path getFilePath(String fileLocation) {
        fileLocation = fileLocation.replace("~", "");
        if (!fileLocation.startsWith("/")) {
            fileLocation = "/" + fileLocation;
        }
        String home = System.getenv("HOME");
        if (!fileLocation.startsWith(home)) {
            fileLocation = home + fileLocation;
        }
        return Paths.get(fileLocation);
    }
}
