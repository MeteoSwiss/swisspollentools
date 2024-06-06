from swisspollentools.utils.schemas import SchemaDict, SchemaTuple,NoneType
from datetime import datetime

# ## Computed Data Schema

# ### Holography Schema



class cdParticleMapSchema(SchemaDict):
    keys = ("frame_location", "frame_location_3d")
    dtypes = (list, list)

class cdParticleMapContainer(SchemaTuple):
    dtypes = (cdParticleMapSchema, )

class cdRecMagPropertiesSchema(SchemaDict):
    keys = ("area", "bbox_area", "convex_area", "eccentricity", "equivalent_diameter", "feret_diameter_max", "major_axis_length", "minor_axis_length", "max_intensity", "min_intensity", "mean_intensity", "orientation", "perimeter", "perimeter_crofton", "solidity")
    dtypes = (int, int, int, float, float, float, float, float, float, float, float, float, float, float, float)

class cdImagePropertiesSchema(SchemaDict):
    keys = ("rec_mag_properties", "frame_location", "rec_distance_estimate", "rec_distance", "frame_location_3d", "physical_location", "focus_graph", "focus_graph_sharpness_measure_name", "rec_mag")
    dtypes = (cdRecMagPropertiesSchema, list, float, float, list, list, list, str, str)


class cdImagePairsSchema(SchemaTuple):
   # dtypes = ((NoneType,cdImagePropertiesSchema), (NoneType,cdImagePropertiesSchema))
    dtypes = (cdImagePropertiesSchema, cdImagePropertiesSchema)

class cdImagePairsContainer(SchemaTuple):
    dtypes = (cdImagePairsSchema, )


class cdHolographySchema(SchemaDict):
    keys = ("saturated_pixels", "particle_location", "particle_map", "image_pairs", "acquisition_distance", "velocity", "velocity_weight", "camera_offset")
    dtypes = (list, list, list, cdImagePairsContainer, float, float, float, NoneType)

# ### Trigger Schema

class cdAnalysisResultsSchema(SchemaDict):
    keys = ("found_peaks", "found_good_peaks", "post_trigger_peak_samples", "valid", "dark_level", "ld_trig0_bg_level", "ld_trig1_bg_level")
    dtypes = (list, list, int, bool, int, int, int)

class cdTriggerSchema(SchemaDict):
    keys = ("velocity", "velocity_weight", "velocity_diff", "analysis_results")
    dtypes = (float, float, float, cdAnalysisResultsSchema)


# ### Fluorescence Schema


class cdCorrelatorDataSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows")
    dtypes = (list, list, list, NoneType, list)

class cdDarkCurrentCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, NoneType, list, dict)

class cdStraylightCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, list, list, dict)

class cdPhaseModulationCorrected(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, list, list, dict)

class cdFullyCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, NoneType, list, list, NoneType, dict)

class cdSpectraSchema(SchemaDict):
    keys = ("method", "formula_tex", "applied_filters", "channel_names", "channel_wavelengths", "excitation_sources", "relative_spectra", "average_mean", "average_std", "average_source_sum")
    dtypes = (str, str, list, list, list, list, list, list, list, list)

class cdProcessedDataSchema(SchemaDict):
    keys = ("dark_current_corrected", "straylight_corrected", "phase_modulation_corrected", "fully_corrected", "spectra", "lifetime", "fluorophore_spectra")
    dtypes = (cdDarkCurrentCorrectedSchema, cdStraylightCorrectedSchema, cdPhaseModulationCorrected, cdFullyCorrectedSchema, cdSpectraSchema, NoneType, NoneType)


class cdFluorescenceSchema(SchemaDict):
    keys = ("correlator_data", "processed_data")
    dtypes = (cdCorrelatorDataSchema, cdProcessedDataSchema)


# ### Final Schema

class computedDataSchema(SchemaDict):
    keys = ("trigger", "holography", "fluorescence" )
    dtypes = (cdTriggerSchema, cdHolographySchema, cdFluorescenceSchema)

# ## Raw Data Schema

# ### Trigger Dump Schema

class rdTriggerDumpSchema(SchemaDict):
    keys = (
        "time_linspace",
        "samples"
    )
    dtypes = (
        list,
        str
    )

# ### Fluorescence Schema

class rdAdcDumpSchema(SchemaDict):
    keys = ("channel_names","time_linspace", "channel_wavelengths", "samples")
    dtypes = (list, list, list, str)

class rdFluorescenceSchema(SchemaDict):
    keys = ("channel_names","adc_dump", "channel_wavelengths", "correlator")
    dtypes = (list, rdAdcDumpSchema, list, list)

# ### Polarization Schema

class rdPolarizationSchema(SchemaDict):
    keys = ("time_linspace", "samples")
    dtypes = (list, str)

# ### Debug Schema

class rdDebugSchema(SchemaDict):
    keys = ("processing_delays", )
    dtypes = (dict,)

# ### Final Schema

class rawDataSchema(SchemaDict):
    keys = ("polarization",
        "sequencer_flags",
        "hw_timestamp",
        "fluorescence",
        "trigger_dump",
        "hw_timestamp_millis",
        "trigger_peak_delay",
        "background",
        "debug",
        "complete"
    )
    dtypes = (rdPolarizationSchema,
        int,
        float,
        rdFluorescenceSchema,
        rdTriggerDumpSchema,
        int,
        float,
        bool,
        dict,
        bool
    )

# ## Classification Schema

# ### Particle Groups Schema

class clsParticleGroupsSchema(SchemaDict):
    keys = ("convex_min_d16", "convex_min_d12", "all_holo", "sphere", "min_d15", "fl_all_sources")
    dtypes = (bool, bool, bool, bool, bool, bool)


# ### Final Schema


class classificationSchema(SchemaDict):
    keys = ("particle_groups", "classifications")
    dtypes = (dict, list)

# ## Metadata Schema

class metaDataSchema(SchemaDict):
    keys = ("poleno_id", "serial", "poleno_variant", "system_location_lat_lon", "system_location_address", "event_base_name", "measurement_campaign", "operating_mode")
    dtypes = (str, str, str, NoneType, str, str, str, NoneType)

# Final Event Schema

class eventSchema(SchemaDict):
    keys = ("timestamp_dt", "uuid", "event_multiplier", "save_probability", "save_event", "raw_data", "computed_data", "classification", "metadata", "system_status", "debug")
    dtypes = (str, str, float, float, bool, rawDataSchema, computedDataSchema, classificationSchema, metaDataSchema, dict, dict)


    def __post_init__(self):
        self["timestamp_dt"] = datetime.strptime(self["timestamp_dt"], "%Y-%m-%d_%H.%M.%S.%f").timestamp()

spt_translation = {
    "metaData/utcEvent": "timestamp_dt",
    "metaData/EventId": "uuid",
    "metaData/eventBaseName": "metadata/event_base_name",
    "recProperties/0/area": "computed_data/holography/image_pairs/0/0/rec_mag_properties/area",
    "recProperties/0/solidity": "computed_data/holography/image_pairs/0/0/rec_mag_properties/solidity",
    "recProperties/0/majorAxis": "computed_data/holography/image_pairs/0/0/rec_mag_properties/major_axis_length",
    "recProperties/0/minorAxis": "computed_data/holography/image_pairs/0/0/rec_mag_properties/minor_axis_length",
    "recProperties/0/perimeter": "computed_data/holography/image_pairs/0/0/rec_mag_properties/perimeter",
    "recProperties/0/eccentricity": "computed_data/holography/image_pairs/0/0/rec_mag_properties/eccentricity",
    "recProperties/1/area": "computed_data/holography/image_pairs/0/1/rec_mag_properties/area",
    "recProperties/1/solidity": "computed_data/holography/image_pairs/0/1/rec_mag_properties/solidity",
    "recProperties/1/majorAxis": "computed_data/holography/image_pairs/0/1/rec_mag_properties/major_axis_length",
    "recProperties/1/minorAxis": "computed_data/holography/image_pairs/0/1/rec_mag_properties/minor_axis_length",
    "recProperties/1/perimeter": "computed_data/holography/image_pairs/0/1/rec_mag_properties/perimeter",
    "recProperties/1/eccentricity": "computed_data/holography/image_pairs/0/1/rec_mag_properties/eccentricity",
    "fluoData/average_std": "computed_data/fluorescence/processed_data/spectra/average_std",
    "fluoData/average_mean": "computed_data/fluorescence/processed_data/spectra/average_mean",
    "fluoData/relative_spectra": "computed_data/fluorescence/processed_data/spectra/relative_spectra"
}
