from swisspollentools.utils.schemas import SchemaDict, SchemaTuple,NoneType

class TriggerDumpSchema(SchemaDict):
    keys = ("time_linspace", "samples")
    dtypes = (list, str)

class AdcDumpSchema(SchemaDict):
    keys = ("channel_names", "channel_wavelengths", "time_linspace", "samples")
    dtypes = (list, list, list, str)

class FluorescenceSchema(SchemaDict):
    keys = ("channel_names", "channel_wavelengths", "correlator", "adc_dump")
    dtypes = (list, list, list, AdcDumpSchema)

class PolarizationSchema(SchemaDict):
    keys = ("time_linspace", "samples")
    dtypes = (list, str)

class DebugSchema(SchemaDict):
    keys = ("fl_dark_signal_range", )
    dtypes = (list, )

class rawDataSchema(SchemaDict):
    keys = ("hw_timestamp", "hw_timestamp_millis", "background", "sequencer_flags", "complete", "trigger_peak_delay", "trigger_dump", "fluorescence", "polarization", "debug")
    dtypes = (float, int, bool, int, bool, float, TriggerDumpSchema, FluorescenceSchema, PolarizationSchema, DebugSchema)
