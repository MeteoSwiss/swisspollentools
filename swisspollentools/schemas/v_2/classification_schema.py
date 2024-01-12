from swisspollentools.utils.schemas import SchemaDict

class ParticleGroupsSchema(SchemaDict):
    keys = ("convex_min_d16", "convex_min_d12", "all_holo", "sphere", "min_d15", "fl_all_sources")
    dtypes = (bool, bool, bool, bool, bool, bool)

class classificationSchema(SchemaDict):
    keys = ("particle_groups", "classifications")
    dtypes = (ParticleGroupsSchema, list)