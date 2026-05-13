from feast import Entity

card_id = Entity(name="card_id", value_type=Entity.ValueType.STRING, description="Payment card identifier")
merchant_id = Entity(name="merchant_id", value_type=Entity.ValueType.STRING, description="Merchant account ID")
device_id = Entity(name="device_id", value_type=Entity.ValueType.STRING, description="Device fingerprint token")
