"""
Constants for the SwissPollenTools library.

This module defines various constants used in the SwissPollenTools library.

Attributes:
    EXTRACTION_WORKER_PREFIX (str): Prefix for extraction worker.
    INFERENCE_WORKER_PREFIX (str): Prefix for inference worker.
    MERGE_WORKER_PREFIX (str): Prefix for merge worker.
    TOCSVW_WORKER_PREFIX (str): Prefix for ToCSV worker.
    TOHDF5_WORKER_PREFIX (str): Prefix for ToHDF5 worker.
    TRAIN_WORKER_PREFIX (str): Prefix for train worker.
    
    KEY_SEP (str): Separator for keys in composite keys.
    ATTRIBUTE_SEP (str): Separator for attributes in composite keys.
    
    HEADER_KEY (str): Key for header data.
    BODY_KEY (str): Key for body data.
    
    _REQUEST_TYPE_KEY (str): Key for request type (private).
    _FILE_PATH_KEY (str): Key for file path (private).
    _BATCH_ID_KEY (str): Key for batch ID (private).
    _N_ITEMS_KEY (str): Key for number of items (private).
    _METADATA_KEY (str): Key for metadata (private).
    _FLUODATA_KEY (str): Key for fluodata (private).
    _REC_PROPERTIES_KEY (str): Key for record properties (private).
    _REC_KEY (str): Key for record data (private).
    _PREDICTION_KEY (str): Key for prediction data (private).
    _LABEL_KEY (str): Key for label data (private).
    
    REQUEST_TYPE_KEY (str): Composite key for request type.
    FILE_PATH_KEY (str): Composite key for file path.
    N_ITEMS_KEY (str): Composite key for number of items.
    BATCH_ID_KEY (str): Composite key for batch ID.
    METADATA_KEY (str): Composite key for metadata.
    FLUODATA_KEY (str): Composite key for fluodata.
    REC_PROPERTIES_KEY (str): Composite key for record properties.
    REC0_PROPERTIES_KEY (str): Composite key for record 0 properties.
    REC1_PROPERTIES_KEY (str): Composite key for record 1 properties.
    REC_KEY (str): Composite key for record data.
    REC0_KEY (str): Composite key for record 0 data.
    REC1_KEY (str): Composite key for record 1 data.
    PREDICTION_KEY (str): Composite key for prediction data.
    LABEL_KEY (str): Composite key for label data.

    END_OF_TASK_VALUE (str): Value indicating the end of a task.
    END_OF_PROCESS_VALUE (str): Value indicating the end of a process.
    EXPECTED_N_ITEMS_VALUE (str): Value indicating the expected number of items.
    EXTRACTION_REQUEST_VALUE (str): Value for extraction request.
    EXTRACTION_RESPONSE_VALUE (str): Value for extraction response.
    INFERENCE_REQUEST_VALUE (str): Value for inference request.
    INFERENCE_RESPONSE_VALUE (str): Value for inference response.
    MERGE_REQUEST_VALUE (str): Value for merge request.
    MERGE_RESPONSE_VALUE (str): Value for merge response.
    TOCSV_REQUEST_VALUE (str): Value for ToCSV request.
    TOCSV_RESPONSE_VALUE (str): Value for ToCSV response.
    TOHDF5_REQUEST_VALUE (str): Value for ToHDF5 request.
    TOHDF5_RESPONSE_VALUE (str): Value for ToHDF5 response.
    TRAIN_REQUEST_VALUE (str): Value for train request.
    TRAIN_RESPONSE_VALUE (str): Value for train response.

    LAUNCH_SLEEP_TIME (int): Sleep time for launch.

    POLLENO_EVENT_SUFFIX (str): Suffix for Polleno event data files.
    POLLENO_REC0_SUFFIX (str): Suffix for Polleno record 0 image files.
    POLLENO_REC1_SUFFIX (str): Suffix for Polleno record 1 image files.

    _NP_ARRAY_DATA_KEYS (tuple): Tuple of private keys for NumPy array data.
    NP_ARRAY_DATA_KEYS (tuple): Tuple of keys for NumPy array data.

    SIPM_DATA_MAX_ITEMS (int): Maximum number of iters in SIMP schema 
        definition
"""

EXTRACTION_WORKER_PREFIX = "exw"
INFERENCE_WORKER_PREFIX = "inw"
MERGE_WORKER_PREFIX = "mew"
TOCSVW_WORKER_PREFIX = "tocsvw"
TOHDF5_WORKER_PREFIX = "tohdf5w"
TRAIN_WORKER_PREFIX = "trw"

GET_ATTRIBUTE_PREFIX = "get"

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
TRAIN_REQUEST_VALUE = "TrainRequest"
TRAIN_RESPONSE_VALUE = "TrainResponse"

LAUNCH_SLEEP_TIME=5

VALID_POLENO_EVENT_SUFFIX = [
    "_event.json",
    "_ev.json"
]
VALID_POLENO_REC0_SUFFIX = [
    "_rec0.png",
    "_ev.computed_data.holography.image_pairs.0.0.rec_mag.png"
]
VALID_POLENO_REC1_SUFFIX = [
    "_rec1.png",
    "_ev.computed_data.holography.image_pairs.0.1.rec_mag.png"
]

_NP_ARRAY_DATA_KEYS = (
    _FLUODATA_KEY + "/",
    _REC_KEY + "/",
    _PREDICTION_KEY + "/"
)
NP_ARRAY_DATA_KEYS = (FLUODATA_KEY, REC_KEY, PREDICTION_KEY)

SIPM_DATA_MAX_ITEMS = 50