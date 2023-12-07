import tensorflow as tf

from swisspollentools.utils import *
from swisspollentools.workers import \
    CSVExtraction, ExtractionRequest, ExtractionWorkerConfig, \
    Train, TrainRequest, TrainWorkerConfig

class MyModel(tf.keras.Model):
    def __init__(self):
        super().__init__()
        self.fluo_cls = tf.keras.Sequential([
            tf.keras.layers.InputLayer(input_shape=(3, 5)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10)
        ])
    
    def call(self, inputs, training=False):
        return self.fluo_cls(inputs["fluorescence_data"]["relative_spectra"])

exw_config = ExtractionWorkerConfig(
    exw_batch_size=32,
)
trw_config = TrainWorkerConfig(
    trw_batch_size=8,
    trw_from_fluorescence_keys=["relative_spectra"]
)

model=MyModel()
n_categories=10
optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3)
loss=tf.keras.losses.CategoricalCrossentropy()

out = ExtractionRequest(file_path="./database/test.csv")
for el in CSVExtraction(out, exw_config):
    request = TrainRequest(file_path="./database/test.csv", batch_id=None, response=el)
    for el in Train(
        request,
        trw_config,
        model=model,
        n_categories=n_categories,
        optimizer=optimizer,
        loss=loss
    ):
        print(el)
        break
    break