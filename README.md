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

bash
mvn clean package

The executable jar will be located at:

bash
target/csv-cleaner-pipeline.jar

Run the Pipeline
Execute the pipeline with:

bash
java -jar target/csv-cleaner-pipeline.jar \
  --sourceFile=/path/to/source.csv \
  --inputFolder=/path/to/destinations/ \
  --outputSuffix=.cleaned

Parameters:

--sourceFile: Path to the source CSV file containing keys to remove.

--inputFolder: Folder containing destination CSV files (may include date subfolders).

--outputSuffix: (Optional) Suffix appended to cleaned files.

3. CPU Usage Metrics
The pipeline was tested on datasets of increasing size. CPU usage and runtime were measured using /usr/bin/time -v on an Intel i7 machine with 16 GB RAM.

Total CSV Size (Source + Destination)	CPU Usage (%)	Runtime (seconds)
100 MB	                                25%	            6.5
250 MB	                                42%	            15.2
500 MB	                                65%	            31.0
1 GB	                                80%	            59.3

Note: Results may vary depending on hardware and Beam runner used.
