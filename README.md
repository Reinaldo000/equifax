# Apache Beam CSV Cleaner Pipeline

## 1. Implementation Details

This Apache Beam pipeline performs the following:

- Reads a list of keys (`dataKeys`) from a source CSV file.
- Reads multiple destination CSV files organized in date-based folders.
- Removes rows from the destination files where the dataKey matches any key from the source file.
- Writes the cleaned destination CSV files back to the same folder with a configurable suffix.
- Original files can be deleted or overwritten after verification.

**Technology stack:**

- Apache Beam Java SDK
- Java 11+
- Maven for build management
- DirectRunner for local execution (can be adapted for cloud runners)

---

## 2. How to Build and Run the Jar

### Build the Jar

Make sure you have Maven and Java installed.

Run:

```bash
mvn clean package
