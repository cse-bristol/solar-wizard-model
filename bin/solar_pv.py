import argparse

from model_solar_pv import model_solar_pv

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Model solar PV")

    parser.add_argument("--pg_uri", metavar="URI", required=True,
                        help="Postgres connection URI. See "
                             "https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6 "
                             "for formatting details")
    parser.add_argument("--solar_dir", metavar="DIR", required=True, help="Directory where temporary files and outputs are stored")
    parser.add_argument("--job_id", metavar="ID", required=True, type=int, help="Albion job ID")
    parser.add_argument("--lidar_paths", metavar="FILE", required=True, action='append', help="All lidar tiles required for modelling")
    parser.add_argument("--horizon_search_radius", default=1000, type=int, metavar="INT", help="Horizon search radius in metres (default 1000)")
    parser.add_argument("--horizon_slices", default=8, type=int, metavar="INT", help="Horizon compass slices (default 8)")
    parser.add_argument("--max_roof_slope_degrees", default=80, type=int, metavar="INT", help="Maximum roof slope for PV (default 80)")
    parser.add_argument("--min_roof_area_m", default=10, type=int, metavar="INT", help="Minimum roof area m² for PV installation (default 10)")
    parser.add_argument("--min_roof_degrees_from_north", default=45, type=int, metavar="INT", help="Minimum degree distance from North for PV (default 45)")
    parser.add_argument("--flat_roof_degrees", default=10, type=int, metavar="INT", help="Angle (degrees) to mount panels on flat roofs (default 10)")

    args = parser.parse_args()

    model_solar_pv(
        pg_uri=args.pg_uri,
        root_solar_dir=args.solar_dir,
        job_id=args.job_id,
        lidar_paths=args.lidar_paths,
        horizon_search_radius=args.horizon_search_radius,
        horizon_slices=args.horizon_slices,
        max_roof_slope_degrees=args.max_roof_slope_degrees,
        min_roof_area_m=args.min_roof_area_m,
        min_roof_degrees_from_north=args.min_roof_degrees_from_north,
        flat_roof_degrees=args.flat_roof_degrees,
    )