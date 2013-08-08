# dbf

Java library for fast reading/writing DBF-files.

## Build from sources

For build project from sources you need to run gradlew script from the root directory:

```sh
git clone git@github.com:jamel/dbf.git
cd dbf
./grablew clean build
```

## dbf-reader

Maven artifact is available from maven central repocitor. Just add dependency in your pom.xml:

```xml
<dependency>
    <groupId>org.jamel.dbf</groupId>
    <artifactId>dbf-reader</artifactId>
    <version>0.0.3</version>
</dependency>
```

### How to use

#### 1. Loading data from small dbf files into collection

In the next example we load data from `streets.dbf` mapping each row to objects of Street class:

```java
public class LoadStreetsExample {

    public static void main(String[] args) {
        File dbf = new File("streets.dbf");
        List<Street> streets = DbfProcessor.loadData(dbf, new DbfRowMapper<Street>() {
            @Override
            public Street mapRow(Object[] row) {
                // here we can change string encoding if it is needed
                String name = new String(DbfUtils.trimLeftSpaces((byte[]) row[0]));
                Integer zip = (Integer) row[1];
                Date createdAt = (Date) row[2];

                return new Street(name, zip, createdAt);
            }
        });

        System.out.println("Streets: " + streets);
    }
}
```

#### 2. Processing each row form dbf file

In the next example we calculate total sum and average price of data from `products.dbf`:

```java
public class PricesCalcExampleV1 {

    public static void main(String[] args) {
        File dbf = new File("products.dbf");
        TotalSumCalculator calc = new TotalSumCalculator();
        DbfProcessor.processDbf(dbf, calc);

        System.out.println("Total sum: " + calc.getTotalSum());
        System.out.println("Average price: " + calc.getTotalSum() / calc.getRowsCount());
    }

    private static class TotalSumCalculator implements DbfRowProcessor {

        private double totalSum;
        private int rowsCount;

        @Override
        public void processRow(Object[] row) {
            // assuming that there are prices in the 4th column
            totalSum += ((Double) row[3]);
            rowsCount++;
        }

        private double getTotalSum() {
            return totalSum;
        }

        private int getRowsCount() {
            return rowsCount;
        }
    }
}
```

#### 3. Print general information of DBF-file

```java
public class DbfInfo {
    public static void main(String[] args) {
        String dbfInfo = DbfProcessor.readDbfInfo(new File("altnames.dbf"))
        System.out.println(dbfInfo);
    }
}
```

Will print:

```
Created at: 13-7-15
Total records: 39906
Header length: 129
Columns:
  #  Name            Type    Length  Decimal
---------------------------------------------
  0  OLDCODE         C       19      0
  1  NEWCODE         C       19      0
  2  LEVEL           C       1       0
```

#### 4. Manually processing rows (low level API)

In the next example we again calculate total sum and average price of data from `products.dbf`. But this time we will manually create DbfReader and iterate throughout each row:

```java
public class PricesCalcExampleV2 {
    public static void main(String[] args) {
        try (DbfReader reader = new DbfReader(new File("products.dbf"))) {
            double totalSum = 0;

            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                // assuming that there are prices in the 4th column
                totalSum += ((Double) row[3]);
            }

            System.out.println("Total sum: " + totalSum);
            System.out.println("Average price: " + totalSum / reader.getHeader().getNumberOfRecords());
        }
    }
}
```

#### 5. Translate DBF to TXT file

If you have no tool for viewing DBF fiels you could simply output all its content to txt file and use your favorite text editor.

```java
public class Dbf2Txt {
    public static void main(String[] args) {
        DbfProcessor.writeToTxtFile(
            new File("altnames.dbf"),
            new File("altnames.txt"),
            Charset.forName("cp866"));
    }
}
```

## dbf-writer

Dbf writing functionality currently is not available.


