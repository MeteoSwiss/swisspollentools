import glob
import re

from swisspollentools.utils import *
from swisspollentools.workers import \
    ZipExtraction, ExtractionRequest, ExtractionWorkerConfig, \
    ToCSV, ToCSVRequest, ToCSVWorkerConfig

# 1. Define the list of files to create the database
file_paths = glob.glob("./data/*/record_*.zip")

# 2. Define the list of labels
file_paths_pattern = r"\.\/data\/(?P<label>\w+)\/record_*.zip"
labels = [re.match(file_paths_pattern, p).groupdict()["label"] for p in file_paths]
out_file_paths = [".".join([label, str(i), "csv"]) for i, label in enumerate(labels)]

# 3. Define the configuration
exw_config = ExtractionWorkerConfig(
    exw_batch_size=2048,
    exw_keep_metadata_key=["eventId", "eventBaseName", "utcEvent"],
    exw_keep_fluorescence_keys=["relative_spectra"],
    exw_keep_rec_properties_keys=["area", "solidity"],
    exw_filters={"min_area": 625, "min_solidity": 0.9}
)
tocsvw_config = ToCSVWorkerConfig(
    tocsvw_output_directory="./database"
)

# 4. Define and instantiate the pipeline
def ToCSVPipeline(config, **kwargs):
    exw_config, tocsvw_config = config

    exw_kwargs = get_subdictionary(kwargs, EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP)
    tocsvw_kwargs = get_subdictionary(kwargs, TOHDF5_WORKER_PREFIX, ATTRIBUTE_SEP)

    def run(file_path, out_file_path):
        out = ExtractionRequest(file_path=file_path)
        out = ZipExtraction(out, exw_config, **exw_kwargs)
        out = (ToCSVRequest(out_file_path, el[BATCH_ID_KEY], response=el) for el in out)
        out = (ToCSV(el, tocsvw_config, **tocsvw_kwargs).__next__() for el in out)

        return list(out)
    
    return run

pipeline = ToCSVPipeline(
    (exw_config, tocsvw_config),
)

# 5. Run the pipeline on the selected input files
for i, (file_path, out_file_path) in enumerate(zip(file_paths, out_file_paths)):
    print(f"Task {i}\n\tfile: {file_path}\n\tout file: {out_file_path}")
    pipeline(file_path=file_path, out_file_path=out_file_path)
