from collections.abc import Mapping

from pyspark.sql import Column

from silver.standardisation.lookup import map_lookup


def normalise_country(col: Column, aliases: Mapping[str, str]) -> Column:
    """Map a country name or code to its canonical code, e.g. UK or England -> GB. Unknown -> NULL."""
    return map_lookup(col, aliases, "upper")
