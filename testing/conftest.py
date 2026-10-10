import pytest
from pipeline.session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a SparkSession for testing."""
    spark = get_spark_session(app_name = "test_session")
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def sample_df(spark):
    return spark.createDataFrame(
        [
            ("James", "Jones", 30),
            ("Ben", "Johnson", 12),
            ("Ellie", "Smith", 30),
            ("Anthony", "Naylor", 26),
            ("Dan", None, 30),
            # TODO: inject more edge cases here

        ],
        ["first_name", "last_name", "age"],
    )