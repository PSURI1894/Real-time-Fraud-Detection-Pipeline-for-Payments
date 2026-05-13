from datetime import timedelta
from feast import FeatureView, Field
from feast.types import Float64, Int64
from entities import card_id, merchant_id

card_features_view = FeatureView(
    name="card_velocity_features",
    entities=[card_id],
    ttl=timedelta(days=30),
    schema=[
        Field(name="recent_card_velocity_1m", dtype=Int64),
        Field(name="card_amount_sum_1h", dtype=Float64),
    ],
    online=True,
    source=None,
)
