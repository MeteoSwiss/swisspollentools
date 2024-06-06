import swisspollentools.schemas.v_0 as v_0
import swisspollentools.schemas.v_1 as v_1
import swisspollentools.schemas.v_neptune as v_neptune
import swisspollentools.schemas.v_jupiter as v_jupiter
import swisspollentools.schemas.v_basic as v_basic
import swisspollentools.schemas.v_jupiter_2024 as v_jupiter_2024

import swisspollentools.schemas.sptSchema as sptSchema
from swisspollentools.utils.schemas import get_auto_caster

auto_caster = get_auto_caster(
    sptSchema.eventSchema, 
    [v_1.eventSchema, v_basic.eventSchema, v_neptune.eventSchema,v_jupiter.eventSchema, v_jupiter_2024.eventSchema],
    [v_1.spt_translation, v_basic.spt_translation, v_neptune.spt_translation, v_jupiter.spt_translation, v_jupiter_2024.spt_translation]
)
