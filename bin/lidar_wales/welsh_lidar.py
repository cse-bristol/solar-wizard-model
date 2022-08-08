"""Generate URLs for downloading Welsh LiDAR - see welsh_lidar.sh for full script"""
from os.path import join

from typing import List

from albion_models import paths

# Tile list extracted from inline javascript in page at
# http://lle.gov.wales/GridProducts#data=LidarCompositeDataset
# (search for GridSquares)
LIDAR_TILES = ["SM70", "SM71", "SM72", "SM73", "SM80", "SM81", "SM82", "SM83", "SM90",
               "SM91", "SM92", "SM93", "SS09", "SN00", "SN01", "SN02", "SN03", "SN04",
               "SS19", "SN10", "SN11", "SN12", "SN13", "SN14", "SN15", "SH12", "SH13",
               "SN20", "SN21", "SN22", "SN23", "SN24", "SN25", "SH22", "SH23", "SH24",
               "SH27", "SH28", "SS39", "SN30", "SN31", "SN32", "SN33", "SN34", "SN35",
               "SN36", "SH32", "SH33", "SH34", "SH35", "SH36", "SH37", "SH38", "SH39",
               "SS48", "SS49", "SN40", "SN41", "SN42", "SN43", "SN44", "SN45", "SN46",
               "SH43", "SH44", "SH45", "SH46", "SH47", "SH48", "SH49", "SS58", "SS59",
               "SN50", "SN51", "SN52", "SN53", "SN54", "SN55", "SN56", "SN57", "SN58",
               "SN59", "SH50", "SH51", "SH52", "SH53", "SH54", "SH55", "SH56", "SH57",
               "SH58", "SS68", "SS69", "SN60", "SN61", "SN62", "SN63", "SN64", "SN65",
               "SN66", "SN67", "SN68", "SN69", "SH60", "SH61", "SH62", "SH63", "SH64",
               "SH65", "SH66", "SH67", "SH68", "SS77", "SS78", "SS79", "SN70", "SN71",
               "SN72", "SN73", "SN74", "SN75", "SN76", "SN77", "SN78", "SN79", "SH70",
               "SH71", "SH72", "SH73", "SH74", "SH75", "SH76", "SH77", "SH78", "SS87",
               "SS88", "SS89", "SN80", "SN81", "SN82", "SN83", "SN87", "SN89", "SH80",
               "SH81", "SH82", "SH83", "SH84", "SH85", "SH86", "SH87", "SH88", "SS96",
               "SS97", "SS98", "SS99", "SN90", "SN91", "SN98", "SN99", "SH90", "SH91",
               "SH92", "SH93", "SH94", "SH95", "SH96", "SH97", "SH98", "ST06", "ST07",
               "ST08", "ST09", "SO00", "SO01", "SO08", "SO09", "SJ01", "SJ02", "SJ03",
               "SJ04", "SJ05", "SJ06", "SJ07", "SJ08", "ST16", "ST17", "ST18", "ST19",
               "SO10", "SO11", "SO18", "SO19", "SJ11", "SJ12", "SJ13", "SJ14", "SJ15",
               "SJ16", "SJ17", "SJ18", "ST26", "ST27", "ST28", "ST29", "SO20", "SO21",
               "SO27", "SO28", "SO29", "SJ20", "SJ21", "SJ22", "SJ23", "SJ24", "SJ25",
               "SJ26", "SJ27", "SJ28", "ST37", "ST38", "ST39", "SO30", "SO31", "SO37",
               "SJ31", "SJ33", "SJ34", "SJ35", "SJ36", "SJ37", "ST47", "ST48", "ST49",
               "SO40", "SO41", "SJ43", "SJ44", "SJ45", "ST58", "ST59", "SO50", "SO51",
               "SJ53", "SJ54"]

RESOLUTIONS = ["50cm", "1m", "2m"]


def create_urls_for_wget(txtfile: str, tiles: List[str], resolutions: List[str]):
    with open(txtfile, "w") as f:
        for tile in tiles:
            for res in resolutions:
                url = f"http://lle.blob.core.windows.net/lidar/{res}_res_{tile}_dsm.zip\n"
                f.write(url)


if __name__ == "__main__":
    file = join(paths.BIN_DIR, "lidar_wales/welsh_lidar.txt")
    create_urls_for_wget(file, LIDAR_TILES, RESOLUTIONS)
