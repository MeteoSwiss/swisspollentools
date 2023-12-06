EXTRACTION_WORKER_PREFIX = "exw"
INFERENCE_WORKER_PREFIX = "inw"
MERGE_WORKER_PREFIX = "mew"
TOCSVW_WORKER_PREFIX = "tocsvw"
TOHDF5_WORKER_PREFIX = "tohdf5w"

KEY_SEP = "/"
ATTRIBUTE_SEP = "_"

HEADER_KEY = "header"
BODY_KEY = "body"

_REQUEST_TYPE_KEY = "request_type"
_FILE_PATH_KEY = "file_path"
_BATCH_ID_KEY  = "batch_id"
_N_ITEMS_KEY = "n_items"
_METADATA_KEY = "metadata"
_FLUODATA_KEY = "fluodata"
_REC_PROPERTIES_KEY = "rec_properties"
_REC_KEY = "rec"
_PREDICTION_KEY = "prediction"
_LABEL_KEY = "label"

REQUEST_TYPE_KEY = KEY_SEP.join([HEADER_KEY, _REQUEST_TYPE_KEY])
FILE_PATH_KEY = KEY_SEP.join([HEADER_KEY, _FILE_PATH_KEY])
N_ITEMS_KEY = KEY_SEP.join([BODY_KEY, _N_ITEMS_KEY])
BATCH_ID_KEY = KEY_SEP.join([HEADER_KEY, _BATCH_ID_KEY])
METADATA_KEY = KEY_SEP.join([BODY_KEY, _METADATA_KEY])
FLUODATA_KEY = KEY_SEP.join([BODY_KEY, _FLUODATA_KEY])
REC_PROPERTIES_KEY = KEY_SEP.join([BODY_KEY, _REC_PROPERTIES_KEY])
REC0_PROPERTIES_KEY = KEY_SEP.join([REC_PROPERTIES_KEY, "0"])
REC1_PROPERTIES_KEY = KEY_SEP.join([REC_PROPERTIES_KEY, "1"])
REC_KEY = KEY_SEP.join([BODY_KEY, _REC_KEY])
REC0_KEY = KEY_SEP.join([REC_KEY, "0"])
REC1_KEY = KEY_SEP.join([REC_KEY, "1"])
PREDICTION_KEY = KEY_SEP.join([BODY_KEY, _PREDICTION_KEY])
LABEL_KEY = KEY_SEP.join([BODY_KEY, _LABEL_KEY])

END_OF_TASK_VALUE = "EndOfTask"
END_OF_PROCESS_VALUE = "EndOfProcess"
EXPECTED_N_ITEMS_VALUE = "ExpectedNItems"
EXTRACTION_REQUEST_VALUE = "ExtractionRequest"
EXTRACTION_RESPONSE_VALUE = "ExtractionResponse"
INFERENCE_REQUEST_VALUE = "InferenceRequest"
INFERENCE_RESPONSE_VALUE = "InferenceResponse"
MERGE_REQUEST_VALUE = "MergeRequest"
MERGE_RESPONSE_VALUE = "MergeResponse"
TOCSV_REQUEST_VALUE = "ToCSVRequest"
TOCSV_RESPONSE_VALUE = "ToCSVResponse"
TOHDF5_REQUEST_VALUE = "ToHDF5Request"
TOHDF5_RESPONSE_VALUE = "ToHDF5Response"

LAUNCH_SLEEP_TIME=5

POLLENO_EVENT_SUFFIX = "_event.json"
POLLENO_REC0_SUFFIX = "_rec0.png"
POLLENO_REC1_SUFFIX = "_rec1.png"