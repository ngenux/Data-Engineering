from classes.transformer_classes import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    # Read Data
    sample_df = spark.read.csv("data/sample_df.csv", header='true', inferSchema='true')

    # Instantiate objects for the classes
    idex = IdExtractor()
    yex = YearExtractor('year')
    cti = CastToInteger('day', 'month', 'year')
    dob = GenerateDob('day', 'month', 'year')
    dd = DropDuplicates(["name", "DOB"])

    # Call the pipeline
    FeaturesPipeline = Pipeline(stages=[idex, yex, cti, dob, dd])
    Featpip = FeaturesPipeline.fit(sample_df)

    # Generate the dataframe
    sample_df = Featpip.transform(sample_df)
    sample_df.show()
