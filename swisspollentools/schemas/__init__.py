from swisspollentools.schemas.v_0 import eventSchema as v_0Schema
from swisspollentools.schemas.v_1 import eventSchema as v_1Schema

from swisspollentools.utils.schemas import get_auto_caster

translation_v_0_to_v_1 = {
    "metadata/device": "metadata/device",
    "metadata/eventId": "metadata/eventId",
    "metadata/utcJson": "metadata/utcJson",
    "metadata/deviceId": "metadata/deviceId",
    "metadata/location/address": "metadata/location/address",
    "metadata/location/altitude": "metadata/location/altitude",
    "metadata/location/latitude": "metadata/location/latitude",
    "metadata/location/longitude": "metadata/location/longitude",
    "metadata/utcEvent": "metadata/utcEvent",
    "metadata/deviceVariant": "metadata/deviceVariant",
    "metadata/eventBaseName": "metadata/eventBaseName",
    "computedData/trigger/peakMax": "computedData/trigger/peakMax",
    "computedData/trigger/peakArea": "computedData/trigger/peakArea",
    "computedData/trigger/peakWidth": "computedData/trigger/peakWidth",
    "computedData/trigger/saturation": "computedData/trigger/saturation",
    "computedData/trigger/trigVelocity": "computedData/trigger/trigVelocity",
    "computedData/lastUpdate": "computedData/lastUpdate",
    "computedData/img0Properties/area": "computedData/img0Properties/area",
    "computedData/img0Properties/solidity": "computedData/img0Properties/solidity",
    "computedData/img0Properties/majorAxis": "computedData/img0Properties/majorAxis",
    "computedData/img0Properties/minorAxis": "computedData/img0Properties/minorAxis",
    "computedData/img0Properties/perimeter": "computedData/img0Properties/perimeter",
    "computedData/img0Properties/coordinates": "computedData/img0Properties/coordinates",
    "computedData/img0Properties/eccentricity": "computedData/img0Properties/eccentricity",
    "computedData/img0Properties/maxIntensity": "computedData/img0Properties/maxIntensity",
    "computedData/img0Properties/minIntensity": "computedData/img0Properties/minIntensity",
    "computedData/img0Properties/meanIntensity": "computedData/img0Properties/meanIntensity",
    "computedData/img1Properties/area": "computedData/img1Properties/area",
    "computedData/img1Properties/solidity": "computedData/img1Properties/solidity",
    "computedData/img1Properties/majorAxis": "computedData/img1Properties/majorAxis",
    "computedData/img1Properties/minorAxis": "computedData/img1Properties/minorAxis",
    "computedData/img1Properties/perimeter": "computedData/img1Properties/perimeter",
    "computedData/img1Properties/coordinates": "computedData/img1Properties/coordinates",
    "computedData/img1Properties/eccentricity": "computedData/img1Properties/eccentricity",
    "computedData/img1Properties/maxIntensity": "computedData/img1Properties/maxIntensity",
    "computedData/img1Properties/minIntensity": "computedData/img1Properties/minIntensity",
    "computedData/img1Properties/meanIntensity": "computedData/img1Properties/meanIntensity",
    "computedData/contextVersionId": "computedData/contextVersionId",
    "computedData/particleProperties/imgVelocity": "computedData/particleProperties/imgVelocity",
    "computedData/particleProperties/estimatedVolume": "computedData/particleProperties/estimatedVolume",
    "classification/label": "classification/label",
    "classification/classifications": "classification/classifications"
}

auto_caster = get_auto_caster(
    v_1Schema, 
    [v_0Schema],
    [translation_v_0_to_v_1]
)