"""
example_data = {
    "metaData": {
        "eventId": <str>,
        "utcEvent": <float>,
        "eventBaseName": <str>
    },
    "recProperties": [
        {
            "area": <float>,
            "solidity": <float>,
            "majorAxis": <float>,
            "minorAxis": <float>,
            "perimeter": <float>,
            "coordinates": <list>,
            "eccentricity": <float>,
            "maxIntensity": <float>,
            "minIntensity": <float>,
            "meanIntensity": <float>
        },
        {
            "area": <float>,
            "solidity": <float>,
            "majorAxis": <float>,
            "minorAxis": <float>,
            "perimeter": <float>,
            "coordinates": <list>,
            "eccentricity": <float>,
            "maxIntensity": <float>,
            "minIntensity": <float>,
            "meanIntensity": <float>
        },
    ],
    "fluoData": {
        "average_std": <np.ndarray>,
        "average_mean": <np.ndarray>,
        "relative_spectra": <np.ndarray>
    },
    "rec": [
        <np.ndarray>,
        <np.ndarray>
    ],
    "prediction": <np.ndarray|dict>
}
"""

import numpy as np

from swisspollentools.utils.schemas import SchemaDict, SchemaTuple, NoneType


class metaDataSchema(SchemaDict):
    keys = ("eventId", "utcEvent", "eventBaseName")
    dtypes = (str, (str,float), str)

class recPropertiesSchema(SchemaDict):
    keys = ("area", "solidity", "majorAxis", "minorAxis", "perimeter", "coordinates", "eccentricity", "maxIntensity", "minIntensity", "meanIntensity")
    dtypes = ((float,int), float, float, float, float, (list, np.ndarray), float, float, float, float)

class recPropertiesCollectionSchema(SchemaTuple):
    dtypes = (recPropertiesSchema, recPropertiesSchema)
    allow_missing_keys=True

class fluoDataSchema(SchemaDict):
    keys = ("average_std", "average_mean", "relative_spectra")
    dtypes = ((list, np.ndarray), (list, np.ndarray), (list, np.ndarray))
    allow_missing_keys = True

class recCollectionSchema(SchemaTuple):
    dtypes = (np.ndarray, np.ndarray)

class eventSchema(SchemaDict):
    keys = ("metaData", "recProperties", "fluoData", "rec", "prediction")
    dtypes = (metaDataSchema, recPropertiesCollectionSchema, fluoDataSchema, recCollectionSchema, (np.ndarray, dict))
    allow_missing_keys = True
