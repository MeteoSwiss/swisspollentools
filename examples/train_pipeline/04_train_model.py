import tensorflow as tf

from swisspollentools.utils import *
from swisspollentools.workers import \
    CSVExtraction, ExtractionRequest, ExtractionWorkerConfig, \
    Merge, MergeRequest, MergeWorkerConfig, \
    Train, TrainRequest, TrainWorkerConfig

n_categories=10
class MyModel(tf.keras.Model):
    def __init__(self):
        super().__init__()
        self.fluo_cls = tf.keras.Sequential([
            tf.keras.layers.InputLayer(input_shape=(3, 5)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(n_categories)
        ])
    
    def call(self, inputs, training=False):
        return self.fluo_cls(inputs["fluorescence_data"]["relative_spectra"])

exw_config = ExtractionWorkerConfig(
    exw_batch_size=2048,
)
mew_config = MergeWorkerConfig()
trw_config = TrainWorkerConfig(
    trw_batch_size=8,
    trw_n_epochs=20,
    trw_from_fluorescence_keys=["relative_spectra"]
)

model=MyModel()
optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3)
loss=tf.keras.losses.CategoricalCrossentropy()

def TrainPipeline(config, **kwargs):
    exw_config, mew_config, trw_config = config
    exw_kwargs = get_subdictionary(kwargs, EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP)
    mew_kwargs = get_subdictionary(kwargs, MERGE_WORKER_PREFIX, ATTRIBUTE_SEP)
    trw_kwargs = get_subdictionary(kwargs, TRAIN_WORKER_PREFIX, ATTRIBUTE_SEP)

    def run(file_path):
        out = ExtractionRequest(file_path=file_path)
        out = CSVExtraction(out, exw_config, **exw_kwargs)
        out = (MergeRequest(file_path, None, el) for el in out)
        out = Merge(list(out), mew_config, **mew_kwargs)
        out = [TrainRequest(file_path, None, out)]
        out = (Train(el, trw_config, **trw_kwargs).__next__() for el in out)

        return list(out)
    
    return run

pipeline = TrainPipeline(
    (exw_config, mew_config, trw_config),
    trw_model=model, trw_optimizer=optimizer, 
    trw_loss=loss, trw_n_categories=n_categories
)

pipeline("./data/train.csv")
