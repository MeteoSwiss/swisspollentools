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
