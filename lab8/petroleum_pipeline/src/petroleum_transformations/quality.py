from pyspark.sql import Column
from pyspark.sql import functions as F


def failed_rules(rules: dict) -> Column:
    checks = [
        F.when(~F.coalesce(F.expr(expression), F.lit(False)), F.lit(name))
        for name, expression in rules.items()
    ]
    return F.filter(F.array(*checks), lambda item: item.isNotNull())