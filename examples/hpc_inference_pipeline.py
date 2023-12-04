import numpy as np
import tensorflow as tf

from swisspollentools.pipelines import InferencePipelineConfig, HPCInferencePipeline

class RandomModel():
    def __init__(self):
        pass

    def predict(self, batch, *args, **kwargs):
        return np.random.random((len(batch["rec0"]), 8))
    
def post_processing_fn(batch):
    predicted_class = np.argmax(batch, axis=-1)
    predicted_certainity = np.max(batch, axis=-1)
    return {
        "class": predicted_class,
        "certainity": predicted_certainity
    }

def main():
    config = InferencePipelineConfig(
        exw_batch_size=1024,
        exw_keep_metadata_key=["eventId"],
        exw_keep_fluorescence_keys=["average_std", "average_mean", "relative_spectra"],
        exw_filters={"max_area": 625, "max_solidity": 0.9},
        inw_from_fluorescence=False,
        inw_batch_size=256,
        inw_post_processing_fn=post_processing_fn,
        tocsvw_output_directory="./tmp"
    )

    pipeline = HPCInferencePipeline(
        config=config,
        n_exw=1,
        n_inw=10,
        n_tocsvw=10,
        ports=[5000, 5001, 5002, 5003, 5004, 5005],
        c_ports=[6000, 6001, 6002],
        s_ports=[6010, 6011, 6012],
        inw_model=RandomModel()
    )

    with tf.device("/cpu:0"):
        pipeline(["./path/to/example.zip"])

    return

if __name__ == "__main__":
    main()
