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


spt_translation = {
    "metaData/eventId": "metadata/eventId",
    "metaData/utcEvent": "metadata/utcEvent",
    "metaData/eventBaseName": "metadata/eventBaseName",
    "recProperties/0/area": "computedData/img0Properties/area",
    "recProperties/0/solidity": "computedData/img0Properties/solidity",
    "recProperties/0/majorAxis": "computedData/img0Properties/majorAxis",
    "recProperties/0/minorAxis": "computedData/img0Properties/minorAxis",
    "recProperties/0/perimeter": "computedData/img0Properties/perimeter",
    "recProperties/0/coordinates": "computedData/img0Properties/coordinates",
    "recProperties/0/eccentricity": "computedData/img0Properties/eccentricity",
    "recProperties/0/maxIntensity": "computedData/img0Properties/maxIntensity",
    "recProperties/0/minIntensity": "computedData/img0Properties/minIntensity",
    "recProperties/0/meanIntensity": "computedData/img0Properties/meanIntensity",
    "recProperties/1/area": "computedData/img1Properties/area",
    "recProperties/1/solidity": "computedData/img1Properties/solidity",
    "recProperties/1/majorAxis": "computedData/img1Properties/majorAxis",
    "recProperties/1/minorAxis": "computedData/img1Properties/minorAxis",
    "recProperties/1/perimeter": "computedData/img1Properties/perimeter",
    "recProperties/1/coordinates": "computedData/img1Properties/coordinates",
    "recProperties/1/eccentricity": "computedData/img1Properties/eccentricity",
    "recProperties/1/maxIntensity": "computedData/img1Properties/maxIntensity",
    "recProperties/1/minIntensity": "computedData/img1Properties/minIntensity",
    "recProperties/1/meanIntensity": "computedData/img1Properties/meanIntensity"
}