from datetime import datetime

from swisspollentools.utils.schemas import SchemaDict
from swisspollentools.schemas.v_2.classification_schema import classificationSchema
from swisspollentools.schemas.v_2.computed_data_schema import computedDataSchema
from swisspollentools.schemas.v_2.metadata_schema import metaDataSchema
from swisspollentools.schemas.v_2.raw_data_schema import rawDataSchema

class eventSchema(SchemaDict):
    keys = ("timestamp_dt", "uuid", "event_multiplier", "save_probability", "save_event", "raw_data", "computed_data", "classification", "metadata", "system_status")
    dtypes = ((str, float), str, float, float, bool, rawDataSchema, computedDataSchema, classificationSchema, metaDataSchema, dict)

    def __post_init__(self):
        self["timestamp_dt"] = datetime.strptime(self["timestamp_dt"], "%Y-%m-%d_%H.%M.%S.%f").timestamp()

spt_translation = {
    "metaData/eventId": "timestamp_dt",
    "metaData/utcEvent": "uuid",
    "metaData/eventBaseName": "metadata/event_base_name",
    "recProperties/0/area": "computed_data/holography/image_pairs/0/0/rec_mag_properties/area",
    "recProperties/0/solidity": "computed_data/holography/image_pairs/0/0/rec_mag_properties/solidity",
    "recProperties/0/majorAxis": "computed_data/holography/image_pairs/0/0/rec_mag_properties/major_axis_length",
    "recProperties/0/minorAxis": "computed_data/holography/image_pairs/0/0/rec_mag_properties/minor_axis_length",
    "recProperties/0/perimeter": "computed_data/holography/image_pairs/0/0/rec_mag_properties/perimeter",
    "recProperties/0/eccentricity": "computed_data/holography/image_pairs/0/0/rec_mag_properties/eccentricity",
    "recProperties/1/area": "computed_data/holography/image_pairs/0/1/rec_mag_properties/area",
    "recProperties/1/solidity": "computed_data/holography/image_pairs/0/1/rec_mag_properties/solidity",
    "recProperties/1/majorAxis": "computed_data/holography/image_pairs/0/1/rec_mag_properties/major_axis_length",
    "recProperties/1/minorAxis": "computed_data/holography/image_pairs/0/1/rec_mag_properties/minor_axis_length",
    "recProperties/1/perimeter": "computed_data/holography/image_pairs/0/1/rec_mag_properties/perimeter",
    "recProperties/1/eccentricity": "computed_data/holography/image_pairs/0/1/rec_mag_properties/eccentricity",
    "fluoData/average_std": "computed_data/fluorescence/processed_data/spectra/average_std",
    "fluoData/average_mean": "computed_data/fluorescence/processed_data/spectra/average_mean",
    "fluoData/relative_spectra": "computed_data/fluorescence/processed_data/spectra/relative_spectra"
}