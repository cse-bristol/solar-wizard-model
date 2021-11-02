from typing import List

import numpy as np
import psycopg2.extras
from psycopg2.sql import SQL, Identifier

from albion_models.db_funcs import sql_script_with_bindings, connect


def model_cost_benefit(pg_uri: str,
                       job_id: int,
                       solar_pv_job_id: int,
                       period_years: int,
                       discount_rate: float,
                       electricity_kwh_costs: List[float],
                       small_inst_cost_per_kwp: float,
                       med_inst_cost_per_kwp: float,
                       large_inst_cost_per_kwp: float,
                       small_inst_fixed_cost: float,
                       med_inst_fixed_cost: float,
                       large_inst_fixed_cost: float,
                       small_inst_vat: float,
                       med_inst_vat: float,
                       large_inst_vat: float):
    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        for electricity_kwh_cost in electricity_kwh_costs:
            _do_model(
                pg_conn,
                job_id=job_id,
                solar_pv_job_id=solar_pv_job_id,
                period_years=period_years,
                discount_rate=discount_rate,
                electricity_kwh_cost=electricity_kwh_cost,
                small_inst_cost_per_kwp=small_inst_cost_per_kwp,
                med_inst_cost_per_kwp=med_inst_cost_per_kwp,
                large_inst_cost_per_kwp=large_inst_cost_per_kwp,
                small_inst_fixed_cost=small_inst_fixed_cost,
                med_inst_fixed_cost=med_inst_fixed_cost,
                large_inst_fixed_cost=large_inst_fixed_cost,
                small_inst_vat=small_inst_vat,
                med_inst_vat=med_inst_vat,
                large_inst_vat=large_inst_vat)
        _create_view(pg_conn, job_id)

        create_cb_report_data(job_id, pg_conn)
    finally:
        pg_conn.close()


def create_cb_report_data(job_id: int, pg_conn):
    # Extract the installation with the best IRR for each toid/elec cost:
    job_id = int(job_id)

    sql_script_with_bindings(
        pg_conn,
        "cb/best-irr.pv-cost-benefit.sql",
        {"job_id": job_id},
        best_irr=SQL(f"models.pv_cb_{job_id}_best_irr_temp"),
        tenure=SQL(f"models.pv_cb_{job_id}_tenure_temp"))

    # Summarise results by IRR band/elec cost/tenure type:
    for tenure in ("Owner occupied",
                   "Privately rented",
                   "Council/housing association",
                   "Non-residential",
                   "Unknown",
                   "All"):
        sql_script_with_bindings(
            pg_conn,
            "cb/summarise.pv-cost-benefit.sql",
            {"job_id": job_id,
             "main_tenure": tenure})


def _do_model(pg_conn,
              job_id: int,
              solar_pv_job_id: int,
              period_years: int,
              discount_rate: float,
              electricity_kwh_cost: float,
              small_inst_cost_per_kwp: float,
              med_inst_cost_per_kwp: float,
              large_inst_cost_per_kwp: float,
              small_inst_fixed_cost: float,
              med_inst_fixed_cost: float,
              large_inst_fixed_cost: float,
              small_inst_vat: float,
              med_inst_vat: float,
              large_inst_vat: float):
    sql_script_with_bindings(
        pg_conn, 'cb/create.pv-cost-benefit.sql', {
            "job_id": job_id,
            "solar_pv_job_id": solar_pv_job_id,
            "period_years": period_years,
            "discount_rate": discount_rate,
            "electricity_kwh_cost": electricity_kwh_cost,
            "small_inst_cost_per_kwp": small_inst_cost_per_kwp,
            "med_inst_cost_per_kwp": med_inst_cost_per_kwp,
            "large_inst_cost_per_kwp": large_inst_cost_per_kwp,
            "small_inst_fixed_cost": small_inst_fixed_cost,
            "med_inst_fixed_cost": med_inst_fixed_cost,
            "large_inst_fixed_cost": large_inst_fixed_cost,
            "small_inst_vat": small_inst_vat,
            "med_inst_vat": med_inst_vat,
            "large_inst_vat": large_inst_vat,
        })

    installations = _get_installations(pg_conn, job_id, electricity_kwh_cost)

    processed = _process_installations(
        installations=installations,
        period_years=period_years,
        discount_rate=discount_rate,
        electricity_kwh_cost=electricity_kwh_cost)

    _update_npv_irr(pg_conn, processed)


def _cash_flow(period_years: int,
               discount_rate: float,
               installation_cost: float,
               electricity_kwh_cost: float,
               total_yield_kwh_year: float) -> List[float]:
    flow = [-installation_cost]
    for year in range(1, period_years + 1):
        flow.append((total_yield_kwh_year * electricity_kwh_cost) / (1 + discount_rate)**year)

    return flow


def _npv(period_years: int,
         discount_rate: float,
         installation_cost: float,
         electricity_kwh_cost: float,
         total_yield_kwh_year: float) -> float:
    """Calculate the Net Present Value (NPV)"""
    flow = _cash_flow(
        period_years=period_years,
        discount_rate=discount_rate,
        installation_cost=installation_cost,
        electricity_kwh_cost=electricity_kwh_cost,
        total_yield_kwh_year=total_yield_kwh_year)
    return sum(flow)


def _irr(period_years: int,
         installation_cost: float,
         electricity_kwh_cost: float,
         total_yield_kwh_year: float) -> float:
    """Calculate the Internal Rate of Return (IRR)"""
    flow = _cash_flow(
        period_years=period_years,
        discount_rate=0,
        installation_cost=installation_cost,
        electricity_kwh_cost=electricity_kwh_cost,
        total_yield_kwh_year=total_yield_kwh_year)

    res = np.roots(flow)
    mask = (res.imag == 0) & (res.real > 0)
    if not mask.any():
        return np.nan
    res = res[mask].real
    rate = res - 1
    rate = rate.item(np.argmin(np.abs(rate)))
    return rate


def _get_installations(pg_conn, job_id: int, electricity_kwh_cost: float) -> List[dict]:
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                job_id, toid, installation_job_id, total_yield_kwh_year, installation_cost
            FROM models.pv_cost_benefit
            WHERE job_id = %(job_id)s AND electricity_kwh_cost = %(electricity_kwh_cost)s
        """, {
            "job_id": job_id,
            "electricity_kwh_cost": electricity_kwh_cost})
        res = cursor.fetchall()
        pg_conn.commit()
        return res


def _process_installations(installations: List[dict],
                           period_years: int,
                           discount_rate: float,
                           electricity_kwh_cost: float) -> List[tuple]:
    return [(
        installation["job_id"],
        installation["toid"],
        installation["installation_job_id"],
        electricity_kwh_cost,
        _npv(
            period_years=period_years,
            discount_rate=discount_rate,
            installation_cost=installation["installation_cost"],
            electricity_kwh_cost=electricity_kwh_cost,
            total_yield_kwh_year=installation["total_yield_kwh_year"]),
        _irr(
            period_years=period_years,
            installation_cost=installation["installation_cost"],
            electricity_kwh_cost=electricity_kwh_cost,
            total_yield_kwh_year=installation["total_yield_kwh_year"])
    ) for installation in installations]


def _update_npv_irr(pg_conn, installations: List[tuple]):
    with pg_conn.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, SQL("""
            UPDATE models.pv_cost_benefit cb
            SET npv = data.npv, irr = data.irr 
            FROM (VALUES %s) AS data (job_id, toid, installation_job_id, electricity_kwh_cost, npv, irr) 
            WHERE cb.job_id = data.job_id
            AND cb.toid = data.toid 
            AND cb.electricity_kwh_cost = data.electricity_kwh_cost 
            AND cb.installation_job_id = data.installation_job_id
        """), argslist=installations)
        pg_conn.commit()


def _create_view(pg_conn, job_id: int):
    with pg_conn.cursor() as cursor:
        cursor.execute(SQL("""
            CREATE OR REPLACE VIEW models.{job_view} AS
            SELECT * FROM models.pv_cost_benefit WHERE job_id = %(job_id)s;
        """).format(
            job_view=Identifier(f"pv_cost_benefit_job_{int(job_id)}")), {
            "job_id": job_id})
        pg_conn.commit()
