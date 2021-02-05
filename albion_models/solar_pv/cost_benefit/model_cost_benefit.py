from typing import List

import numpy as np
import psycopg2.extras
from psycopg2.sql import SQL

from albion_models.db_funcs import sql_script_with_bindings, connect


def model_cost_benefit(pg_uri: str,
                       job_id: int,
                       solar_pv_job_id: int,
                       period_years: int,
                       discount_rate: float,
                       electricity_kwh_cost: float):

    pg_conn = connect(pg_uri, cursor_factory=psycopg2.extras.DictCursor)
    try:
        sql_script_with_bindings(
            pg_conn, 'create.pv_cost_benefit.sql', {
                "job_id": job_id,
                "solar_pv_job_id": solar_pv_job_id,
                "period_years": period_years,
                "discount_rate": discount_rate,
                "electricity_kwh_cost": electricity_kwh_cost,
            })

        installations = _get_installations(pg_conn, job_id)

        processed = _process_installations(
            installations=installations,
            period_years=period_years,
            discount_rate=discount_rate,
            electricity_kwh_cost=electricity_kwh_cost)

        _update_npv_irr(pg_conn, processed)

    finally:
        pg_conn.close()


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


def _get_installations(pg_conn, job_id: int) -> List[dict]:
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                job_id, toid, installation_job_id, total_yield_kwh_year, installation_cost
            FROM models.pv_cost_benefit
            WHERE job_id = %(job_id)s
        """, {"job_id": job_id})
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
            FROM (VALUES %s) AS data (job_id, toid, installation_job_id, npv, irr) 
            WHERE cb.job_id = data.job_id
            AND cb.toid = data.toid 
            AND cb.installation_job_id = data.installation_job_id
        """), argslist=installations)
        pg_conn.commit()


model_cost_benefit("postgresql://albion_webapp:ydBbE3JCnJ4@localhost:32768/albion", 140, 135, 25, 0.035, 0.16)