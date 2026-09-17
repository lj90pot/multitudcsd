from multitudcsd.config import get_spark_session
from multitudcsd.storage import read_delta
from multitudcsd.transforms.silver_to_gold import (
    aggregate_mentions_by_cell_hour,
    count_k_anonymity_effect,
)

sesion = get_spark_session("medir-k")
silver = read_delta(sesion, "silver", "silver_csd_mentions")
print(count_k_anonymity_effect(aggregate_mentions_by_cell_hour(silver)))
sesion.stop()