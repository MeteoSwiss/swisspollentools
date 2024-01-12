from swisspollentools.utils.schemas import SchemaDict, SchemaTuple,NoneType

class ParticleMapSchema(SchemaDict):
    keys = ("frame_location", "frame_location_3d")
    dtypes = (list, list)

class ParticleMapContainer(SchemaTuple):
    dtypes = (ParticleMapSchema, )

class RecMagPropertiesSchema(SchemaDict):
    keys = ("area", "bbox_area", "convex_area", "eccentricity", "equivalent_diameter", "feret_diameter_max", "major_axis_length", "minor_axis_length", "max_intensity", "min_intensity", "mean_intensity", "orientation", "perimeter", "perimeter_crofton", "solidity")
    dtypes = (int, int, int, float, float, float, float, float, float, float, float, float, float, float, float)

class ImagePropertiesSchema(SchemaDict):
    keys = ("rec_mag_properties", "frame_location", "rec_distance_estimate", "rec_distance", "frame_location_3d", "physical_location", "focus_graph", "focus_graph_sharpness_measure_name", "rec_mag")
    dtypes = (RecMagPropertiesSchema, list, float, float, list, list, list, str, str)

class ImagePairsSchema(SchemaTuple):
    dtypes = (ImagePropertiesSchema, ImagePropertiesSchema)

class ImagePairsContainer(SchemaTuple):
    dtypes = (ImagePairsSchema, )

class HolographySchema(SchemaDict):
    keys = ("saturated_pixels", "particle_location", "particle_map", "image_pairs", "acquisition_distance", "velocity", "velocity_weight", "camera_offset")
    dtypes = (list, list, ParticleMapContainer, ImagePairsContainer, float, float, float, NoneType)

class AnalysisResultsSchema(SchemaDict):
    keys = ("found_peaks", "found_good_peaks", "post_trigger_peak_samples", "valid", "dark_level", "ld_trig0_bg_level", "ld_trig1_bg_level")
    dtypes = (list, list, int, bool, int, int, int)

class TriggerSchema(SchemaDict):
    keys = ("velocity", "velocity_weight", "velocity_diff", "analysis_results")
    dtypes = (float, float, float, AnalysisResultsSchema)

class CorrelatorDataSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows")
    dtypes = (list, list, list, NoneType, list)

class DarkCurrentCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, NoneType, list, dict)

class StraylightCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, list, list, dict)

class PhaseModulationCorrected(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, list, list, list, list, dict)

class FullyCorrectedSchema(SchemaDict):
    keys = ("average", "magnitude", "phase_shift", "modulation_index", "overflows", "correction_methods")
    dtypes = (list, NoneType, list, list, NoneType, dict)

class SpectraSchema(SchemaDict):
    keys = ("method", "formula_tex", "applied_filters", "channel_names", "channel_wavelengths", "excitation_sources", "relative_spectra", "average_mean", "average_std", "average_source_sum")
    dtypes = (str, str, list, list, list, list, list, list, list, list)

class ProcessedDataSchema(SchemaDict):
    keys = ("dark_current_corrected", "straylight_corrected", "phase_modulation_corrected", "fully_corrected", "spectra", "lifetime", "fluorophore_spectra")
    dtypes = (DarkCurrentCorrectedSchema, StraylightCorrectedSchema, PhaseModulationCorrected, FullyCorrectedSchema, SpectraSchema, NoneType, NoneType)

class FluorescenceSchema(SchemaDict):
    keys = ("correlator_data", "processed_data")
    dtypes = (CorrelatorDataSchema, ProcessedDataSchema)

class computedDataSchema(SchemaDict):
    keys = ("trigger", "holography", "fluorescence" )
    dtypes = (TriggerSchema, HolographySchema, FluorescenceSchema)