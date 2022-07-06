-- measure costs model, ported from R
-- https://github.com/cse-bristol/794-enfield/blob/master/src/enfield-thermos-buildings.R
DELETE FROM models.measure_costs WHERE job_id = {job_id}

-- assumes pcts as values between 0 and 1
WITH costs AS (
    SELECT
        hd.toid,
        {job_id} AS job_id,
        hd.annual_demand,

        {include_cwi} AND b.cwi_recommended AS cwi_applied,
        {include_swi} AND b.swi_recommended AS swi_applied,
        {include_loft_ins} AND b.loft_ins_recommended AS loft_ins_applied,
        {include_roof_ins} AND b.roof_ins_recommended AS roof_ins_applied,
        {include_floor_ins} AND b.floor_ins_recommended AS floor_ins_applied,
        {include_glazing} AND b.glazing_recommended AS glazing_applied,

        CASE WHEN {include_cwi} AND b.cwi_recommended THEN
            {cwi_max_pct_area} * hd.external_wall_area * {cwi_per_m2_cost} + {cwi_fixed_cost}
        ELSE 0.0 END AS cwi_cost,
        CASE WHEN {include_swi} AND b.swi_recommended THEN
            {swi_max_pct_area} * hd.external_wall_area * {swi_per_m2_cost} + {swi_fixed_cost}
        ELSE 0.0 END AS swi_cost,
        CASE WHEN {include_loft_ins} AND b.loft_ins_recommended THEN
            {loft_ins_max_pct_area} * hd.footprint * {loft_ins_per_m2_cost} + {loft_ins_fixed_cost}
        ELSE 0.0 END AS loft_ins_cost,
        CASE WHEN {include_roof_ins} AND b.roof_ins_recommended THEN
            {roof_ins_max_pct_area} * hd.footprint * {roof_ins_per_m2_cost} + {roof_ins_fixed_cost}
        ELSE 0.0 END AS roof_ins_cost,
        CASE WHEN {include_floor_ins} AND b.floor_ins_recommended THEN
            {floor_ins_max_pct_area} * hd.footprint * {floor_ins_per_m2_cost} + {floor_ins_fixed_cost}
        ELSE 0.0 END AS floor_ins_cost,
        CASE WHEN {include_glazing} AND b.glazing_recommended THEN
            {glazing_max_pct_area} * hd.external_wall_area * {glazing_per_m2_cost} + {glazing_fixed_cost}
        ELSE 0.0 END AS glazing_cost,

        CASE WHEN {include_cwi} AND b.cwi_recommended THEN
            -{cwi_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS cwi_demand_reduction,
        CASE WHEN {include_swi} AND b.swi_recommended THEN
            -{swi_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS swi_demand_reduction,
        CASE WHEN {include_loft_ins} AND b.loft_ins_recommended THEN
            -{loft_ins_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS loft_ins_demand_reduction,
        CASE WHEN {include_roof_ins} AND b.roof_ins_recommended THEN
            -{roof_ins_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS roof_ins_demand_reduction,
        CASE WHEN {include_floor_ins} AND b.floor_ins_recommended THEN
            -{floor_ins_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS floor_ins_demand_reduction,
        CASE WHEN {include_glazing} AND b.glazing_recommended THEN
            -{glazing_pct_demand_reduction} * hd.annual_demand
        ELSE 0.0 AS glazing_demand_reduction
    FROM models.heat_demand hd
    LEFT JOIN aggregates.building b ON hd.toid = b.toid
)
INSERT INTO models.measure_costs
SELECT
    costs.toid,
    costs.job_id,

    costs.cwi_cost +
        costs.swi_cost +
        costs.loft_ins_cost +
        costs.roof_ins_cost +
        costs.floor_ins_cost +
        costs.glazing_cost AS total_cost,
    (costs.cwi_demand_reduction +
        costs.swi_demand_reduction +
        costs.loft_ins_demand_reduction +
        costs.roof_ins_demand_reduction +
        costs.floor_ins_demand_reduction +
        costs.glazing_demand_reduction) / costs.annual_demand AS pct_demand_reduction,
    costs.annual_demand,
    costs.annual_demand +
        costs.cwi_demand_reduction +
        costs.swi_demand_reduction +
        costs.loft_ins_demand_reduction +
        costs.roof_ins_demand_reduction +
        costs.floor_ins_demand_reduction +
        costs.glazing_demand_reduction AS new_annual_demand,

    costs.cwi_applied,
    costs.swi_applied,
    costs.loft_ins_applied,
    costs.roof_ins_applied,
    costs.floor_ins_applied,
    costs.glazing_applied,

    costs.cwi_cost,
    costs.swi_cost,
    costs.loft_ins_cost,
    costs.roof_ins_cost,
    costs.floor_ins_cost,
    costs.glazing_cost,

    costs.cwi_demand_reduction,
    costs.swi_demand_reduction,
    costs.loft_ins_demand_reduction,
    costs.roof_ins_demand_reduction,
    costs.floor_ins_demand_reduction,
    costs.glazing_demand_reduction
FROM costs;
