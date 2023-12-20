import swisspollentools.schemas.v_0 as v_0
import swisspollentools.schemas.v_1 as v_1
import swisspollentools.schemas.sptSchema as sptSchema
from swisspollentools.utils.schemas import get_auto_caster

auto_caster = get_auto_caster(
    sptSchema.eventSchema, 
    [v_0.eventSchema, v_1.eventSchema],
    [v_0.spt_translation, v_1.spt_translation]
)