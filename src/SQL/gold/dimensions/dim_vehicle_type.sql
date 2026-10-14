SELECT
    v.vehicle_type_id,
    v.name AS vehicle_type_name,
    v.form_factor,
    v.propulsion_type,
    v.rider_capacity,
    v.max_range_meters,
    p.plan_id AS pricing_plan_id,
    p.name AS pricing_plan_name,
    p.price,
    p.currency
FROM silver_vehicle_types AS v
LEFT JOIN silver_system_pricing_plans AS p
    ON v.default_pricing_plan_id = p.plan_id
