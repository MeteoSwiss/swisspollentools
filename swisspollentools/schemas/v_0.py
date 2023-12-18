from swisspollentools.utils import SIPM_DATA_MAX_ITEMS, SchemaDict, NoneType

# rawData Schemas
class corrChannelsSchema(SchemaDict):
    keys = ("avg", "ofHits", "corrMag", "corrPha", "windows")
    dtypes = (list, list, list, list, list)

class sipmDataSchema(SchemaDict):
    keys = ("fExc", "sources", "corrChannels", "corrInterval", "utcStartConfig")
    dtypes = ((float, NoneType), list, corrChannelsSchema, (float, NoneType), (float, NoneType))

class sipmDataCollectionSchema(SchemaDict):
    keys = tuple(str(i) for i in range(SIPM_DATA_MAX_ITEMS))
    dtypes = (sipmDataSchema, ) * SIPM_DATA_MAX_ITEMS
    allow_missing_keys = True

class rawDataSchema(SchemaDict):
    keys = ("sipmData", )
    dtypes = (sipmDataCollectionSchema, )

# metadata Schemas
class locationSchema(SchemaDict):
    keys = ("address", "altitude", "latitude", "longitude")
    dtypes = ((str, NoneType), (int, float), (int, float), (int, float))
    allow_missing_keys = True

class metaDataSchema(SchemaDict):
    keys = ("device", "eventId", "utcJson", "deviceId", "location", "utcEvent", "deviceVariant", "eventBaseName")
    dtypes = (str, str, (float, int), str, locationSchema, float, str, str)

# computedData Schemas
class triggerSchema(SchemaDict):
    keys = ("peakMax", "peakArea", "peakWidth", "saturation", "trigVelocity")
    dtypes = (list, list, list, list, float)

class imgPropertiesSchema(SchemaDict):
    keys = ("area", "solidity", "majorAxis", "minorAxis", "perimeter", "coordinates", "eccentricity", "maxIntensity", "minIntensity", "meanIntensity")
    dtypes = ((float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (list, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType))

class particlePropertiesSchema(SchemaDict):
    keys = ("imgVelocity", "estimatedVolume")
    dtypes = ((float, list, NoneType), (str, NoneType))

class computedDataSchema(SchemaDict):
    keys = ("trigger", "lastUpdate", "img0Properties", "img1Properties", "contextVersionId", "particleProperties", "fluorescenceSpectra", "fluorescenceLifetime")
    dtypes = (triggerSchema, (float, NoneType), imgPropertiesSchema, imgPropertiesSchema, (str, NoneType), particlePropertiesSchema, (dict, NoneType), (dict, NoneType))

# classification Schemas
class classificationSchema(SchemaDict):
    keys = ("label", "classifications")
    dtypes = (str, list)

# event Schemas
class eventSchema(SchemaDict):
    keys = ("rawData", "metadata", "computedData", "classification")
    dtypes = (rawDataSchema, metaDataSchema, computedDataSchema, classificationSchema)
