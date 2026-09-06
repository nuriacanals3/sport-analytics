"""Phase 6: transport-scenario carbon comparison -- a posterior layer, NOT
part of the optimiser. Every function here takes miles a schedule has
ALREADY committed to (the real schedule, or any Phase 5 lambda-point loaded
from optimization/artifacts/pareto_results/) and reports what CO2 would have
been under a different mode of transport for the same trips. Nothing here
searches over schedules or changes any date -- it's pure arithmetic on a mile
count that's already fixed, which is why the Streamlit app can call it live
on every toggle without re-running any optimisation.

Charter is the status-quo scenario objectives.py already uses everywhere
else. Commercial and SAF are new to Phase 6.
"""
from optimization.objectives import CHARTER_CO2_PER_MILE_KG, NBA_TRAVELING_PARTY_SIZE

KM_PER_MILE = 1.60934  # 1 mile = 1.60934 km

# Commercial (per-passenger) -- DEFRA 2025 "Business travel - air", economy,
# short-haul (<3,700 km) figure, used as a single blended factor for the
# whole league.
#
# RF (radiative forcing) caveat: this DEFRA figure INCLUDES a
# radiative-forcing uplift for high-altitude non-CO2 effects (contrails,
# NOx). The charter figure below does not -- it's a plain fuel-burn number.
# That's a real methodological mismatch, not a rounding error, and I'm not
# trying to paper over it with a guessed correction factor. Net effect: 
# comparisons against charter below are directionally right (commercial 
# meaningfully lower) but should be read as "roughly X%", not to the given decimal.
DEFRA_ECONOMY_SHORT_HAUL_PER_PAX_KM = 0.126  # kg CO2e/passenger-km, with RF
COMMERCIAL_CO2_PER_PAX_MILE_KG = DEFRA_ECONOMY_SHORT_HAUL_PER_PAX_KM * KM_PER_MILE  # ~0.203

# SAF blend applied to the CHARTER only ("private jet running on SAF"), not a
# standalone travel mode.
#   - 0.50: current ASTM D7566 certification ceiling for drop-in SAF -- the
#     most SAF legally burnable in this exact aircraft today, not real-world
#     uptake (<1% of global jet fuel in 2025/26).
#   - 0.80: typical HEFA-pathway LIFECYCLE (well-to-wake) reduction vs fossil
#     jet fuel (IATA/NREL). CAVEAT: this is a lifecycle figure applied to a
#     combustion baseline -- the one place the strict combustion-basis parity
#     with charter doesn't quite hold either, same spirit as the RF caveat
#     above. Standard simplification; flagged, not hidden.
SAF_BLEND_FRACTION = 0.50
SAF_LIFECYCLE_REDUCTION = 0.80

SCENARIOS = ['charter', 'commercial', 'saf_blend']


def carbon_for_scenario(total_miles, scenario, party_size=NBA_TRAVELING_PARTY_SIZE):
    """total_miles: a schedule's (or a single team's) total miles, as already
    computed by objectives.total_miles() / Phase 5's artifacts -- never
    recomputed here. Returns total kg CO2 for the whole travelling party.

    This is a single point estimate, not a precise measurement -- see the
    module docstring's RF caveat before displaying it to more precision than
    "roughly X%" of some other scenario.
    """
    if scenario == 'charter':
        # Per-aircraft, not per-passenger -- doesn't scale with party_size.
        return total_miles * CHARTER_CO2_PER_MILE_KG
    if scenario == 'commercial':
        return total_miles * COMMERCIAL_CO2_PER_PAX_MILE_KG * party_size
    if scenario == 'saf_blend':
        reduction = SAF_BLEND_FRACTION * SAF_LIFECYCLE_REDUCTION  # ~0.40 off charter
        return total_miles * CHARTER_CO2_PER_MILE_KG * (1 - reduction)
    raise ValueError(f"Unknown scenario: {scenario!r} (expected one of {SCENARIOS})")
