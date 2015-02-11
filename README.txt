Ryan Millay & Shivastuti Koul
CS6240 A2


- There is a pre-compiled jar of the source code in the "target" directory.
  To run:
    hadoop jar hadoop.a2-0.0.1-SNAPSHOT.jar <input path> <output path> <sampling rate> <# of bins> <combiner class name>

  Where:
    input path:            Location of input file
    output path:           Output directory name
    sampling rate:         Elect to process every nth line (integer value)
    # of bins:             Number of partitions/reducers
    combiner class name:   Class name for a combiner (MedianPriceCombiner|TODO).  Leave blank to not use a combiner.

- If you want to compile the source and repackage the jar:
    1. Install maven on your machine
    2. cd to the top directory (it should contain the pom.xml file)
    3. $ mvn clean
    4. $ mvn compile
    5. $ mvn package
