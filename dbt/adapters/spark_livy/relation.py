from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException
import dbt.adapters.spark_livy.cloudera_tracking as tracker


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: SparkQuotePolicy = SparkQuotePolicy()
    include_policy: SparkIncludePolicy = SparkIncludePolicy()
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    information: Optional[str] = None

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException("Cannot set database in spark!")
        if self.type:
            tracker.track_usage(
                {
                    "event_type": "dbt_spark_livy_model_access",
                    "model_name": self.render(),
                    "model_type": self.type,
                    "incremental_strategy": "",
                }
            )

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()

    def log_relation(self, incremental_strategy):
        if self.type:
            tracker.track_usage(
                {
                    "event_type": "dbt_spark_livy_new_incremental",
                    "model_name": self.render(),
                    "model_type": self.type,
                    "incremental_strategy": incremental_strategy,
                }
            )
