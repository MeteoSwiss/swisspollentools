from swisspollentools.utils import SIPM_DATA_MAX_ITEMS, SchemaDict, NoneType

class holoSchema(SchemaDict):
    keys = ("xy", "zFine", "zRough", "utcImageCapture")
    dtypes = (list, (float, NoneType), (float, NoneType), float)

class adcDumpSchema(SchemaDict):
    keys = ("0A", "0B", "1A", "1B", "2A", "2B", "ds", "utcDumpStart")
    dtypes = (list, list, list, list, list, list, float, float)

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

class polAdcSeriesSchema(SchemaDict):
    keys = ("0", "1", "ds", "utcDumpStart")
    dtypes = (list, list, float, float)

class rawDataSchema(SchemaDict):
    keys = ("holo0", "holo1", "valid", "adcDump", "sipmData", "triggTDiff", "polAdcSeries", "utcTriggerEvent", "contextVersionId", "triggProbability")
    dtypes = (holoSchema, holoSchema, int, adcDumpSchema, sipmDataCollectionSchema, float, polAdcSeriesSchema, float, (str, NoneType), float)

class locationSchema(SchemaDict):
    keys = ("address", "altitude", "latitude", "longitude")
    dtypes = ((str, NoneType), (int, float), (int, float), (int, float))
    allow_missing_keys = True

class metaDataSchema(SchemaDict):
    keys = ("device", "eventId", "utcJson", "deviceId", "location", "utcEvent", "deviceVariant", "eventBaseName")
    dtypes = (str, str, (int, float), str, locationSchema, float, str, str)

class triggerSchema(SchemaDict):
    keys = ("peakMax", "peakArea", "peakWidth", "saturation", "trigVelocity")
    dtypes = (list, list, list, list, float)

class imgPropertiesSchema(SchemaDict):
    keys = ("area", "solidity", "majorAxis", "minorAxis", "perimeter", "coordinates", "eccentricity", "maxIntensity", "minIntensity", "meanIntensity")
    dtypes = ((float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (list, NoneType), (float, NoneType), (float, NoneType), (float, NoneType), (float, NoneType))

class particlePropertiesSchema(SchemaDict):
    keys = ("imgVelocity", "estimatedVolume")
    dtypes = ((float, list, NoneType), (str, NoneType))

class meanAverageScaledSchema(SchemaDict):
    keys = ("mean_280", "mean_365", "mean_405")
    dtypes = (list, list, list)

class fluorescenceSpectraSchema(SchemaDict):
    keys = ("method", "average_std", "formula_tex", "average_mean", "channel_names", "applied_filters", "relative_spectra", "excitation_sources", "average_sources_sum", "channel_wavelengths", "mean_average_scaled")
    dtypes = (str, list, str, list, list, list, list, list, list, list, meanAverageScaledSchema)

class initValuesSchema(SchemaDict):
    keys = ("tau1", )
    dtypes = (float, )

class minimizerResultSchema(SchemaDict):
    keys = ("aic", "bic", "cost", "grad", "nfev", "njev", "ndata", "nfree", "chisqr", "method", "nvarys", "redchi", "status", "aborted", "message", "success", "optimality", "init_values")
    dtypes = ((float, NoneType), (float, NoneType), (float, NoneType), list, int, int, int, int, (float, NoneType), str, int, (float, NoneType), int, bool, str, bool, (float, NoneType), initValuesSchema)

class ExpSchema(SchemaDict):
    keys = ("tau", "alpha", "fittingMode", "minimizerResult", "confidenceInterval")
    dtypes = (list, list, str, minimizerResultSchema, list)

class fluorescenceLifetimeSchema(SchemaDict):
    keys = ("1-exp", )
    dtypes = (ExpSchema, )

class computedDataSchema(SchemaDict):
    keys = ("trigger", "lastUpdate", "img0Properties", "img1Properties", "contextVersionId", "particleProperties", "fluorescenceSpectra", "fluorescenceLifetime")
    dtypes = (triggerSchema, (float, NoneType), imgPropertiesSchema, imgPropertiesSchema, (str, NoneType), particlePropertiesSchema, fluorescenceSpectraSchema, fluorescenceLifetimeSchema)

class classificationSchema(SchemaDict):
    keys = ("label", "classifications")
    dtypes = (str, list)

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
    "recProperties/1/meanIntensity": "computedData/img1Properties/meanIntensity",
    "fluoData/average_std": "computedData/fluorescenceSpectra/average_std",
    "fluoData/average_mean": "computedData/fluorescenceSpectra/average_mean",
    "fluoData/relative_spectra": "computedData/fluorescenceSpectra/relative_spectra"
}