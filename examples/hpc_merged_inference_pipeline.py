import numpy as np
import tensorflow as tf

from swisspollentools.pipelines import MergedInferencePipelineConfig, HPCMergedInferencePipeline

class RandomModel(tf.keras.Model):
    def __init__(self, n_categories=8):
        super().__init__()
        self.holo_cls = tf.keras.Sequential([
            tf.keras.layers.InputLayer(input_shape=(200, 200)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(n_categories),
            tf.keras.layers.Softmax()
        ])

        self.merge_cls = tf.keras.layers.Average()

    def call(self, inputs, training=False, *args, **kwargs):
        return self.merge_cls([
            self.holo_cls(inputs["rec0"]),
            self.holo_cls(inputs["rec1"]),
        ])
    
def post_processing_fn(batch):
    predicted_class = np.argmax(batch, axis=-1)
    predicted_certainity = np.max(batch, axis=-1)
    return {
        "class": predicted_class,
        "certainity": predicted_certainity
    }

def main():
    config = MergedInferencePipelineConfig(
        exw_batch_size=1024,
        exw_keep_metadata_key=["eventId"],
        exw_keep_fluorescence_keys=["average_std", "average_mean", "relative_spectra"],
        exw_filters={"max_area": 625, "max_solidity": 0.9},
        inw_from_fluorescence=False,
        inw_batch_size=256,
        inw_post_processing_fn=post_processing_fn,
        tocsvw_output_directory="./tmp"
    )

    pipeline = HPCMergedInferencePipeline(
        config=config,
        n_exw=1,
        n_inw=10,
        n_tocsvw=10,
        ports=list(range(5000, 5010)),
        c_ports=list(range(6000, 6010)),
        s_ports=list(range(6010, 6020)),
        inw_model=RandomModel()
    )

    with tf.device("/cpu:0"):
        pipeline(["/scratch/mdsb/poleno_datasets_with_fluo/alnus/CH_payerne_ers_2023_02_13_1_2023-06-20_04-37-54.zip"])

    return

if __name__ == "__main__":
    main()
