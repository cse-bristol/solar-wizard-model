import argparse

from solar_pv.model_solar_pv import model_solar_pv

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Model solar PV")

    parser.add_argument("--pg-uri", metavar="URI", required=True,
                        help="Postgres connection URI. See "
                             "https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6 "
                             "for formatting details")
    parser.add_argument("--solar-dir", metavar="DIR", required=True, help="Directory where temporary files and outputs are stored")
    parser.add_argument("--job-id", metavar="ID", required=True, type=int, help="Albion job ID")
    parser.add_argument("--lidar-path", metavar="FILE", required=True, action='append', help="All lidar tiles required for modelling")
    parser.add_argument("--horizon-search-radius", default=1000, type=int, metavar="INT", help="Horizon search radius in metres (default 1000)")
    parser.add_argument("--horizon-slices", default=8, type=int, metavar="INT", help="Horizon compass slices (default 8)")
    parser.add_argument("--max-roof-slope-degrees", default=80, type=int, metavar="INT", help="Maximum roof slope for PV (default 80)")
    parser.add_argument("--min-roof-area-m", default=10, type=int, metavar="INT", help="Minimum roof area m² for PV installation (default 10)")
    parser.add_argument("--min-roof-degrees-from-north", default=45, type=int, metavar="INT", help="Minimum degree distance from North for PV (default 45)")
    parser.add_argument("--flat-roof-degrees", default=10, type=int, metavar="INT", help="Angle (degrees) to mount panels on flat roofs (default 10)")
    parser.add_argument("--peak-power-per-m2", default=0.120, type=float, metavar="FLOAT", help="Nominal peak power (kWp) per m² of roof (default 0.120)")
    parser.add_argument("--pv-tech", default="crystSi", metavar="STR", choices=["crystSi", "CIS", "CdTe"], help="PV technology (default crystSi)")

    args = parser.parse_args()
    model_solar_pv(
        pg_uri=args.pg_uri,
        root_solar_dir=args.solar_dir,
        job_id=args.job_id,
        lidar_paths=args.lidar_path,
        horizon_search_radius=args.horizon_search_radius,
        horizon_slices=args.horizon_slices,
        max_roof_slope_degrees=args.max_roof_slope_degrees,
        min_roof_area_m=args.min_roof_area_m,
        min_roof_degrees_from_north=args.min_roof_degrees_from_north,
        flat_roof_degrees=args.flat_roof_degrees,
        peak_power_per_m2=args.peak_power_per_m2,
        pv_tech=args.pv_tech,
    )
