from swisspollentools.utils.schemas import SchemaDict, NoneType

class metaDataSchema(SchemaDict):
    keys = ("poleno_id", "serial", "poleno_variant", "system_location_lat_lon", "system_location_address", "event_base_name", "measurement_campaign", "operating_mode")
    dtypes = (str, str, str, NoneType, str, str, str, NoneType)
