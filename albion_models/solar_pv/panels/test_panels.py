from shapely import wkt

from albion_models.solar_pv.panels.panels import _roof_panels
from albion_models.test.test_funcs import ParameterisedTestCase

_failed = {"roof_plane_id": 4564,
           "roof": 'MULTIPOLYGON(((402174.6689291877 201156.51700193272,402174.94804027333 201157.43181243743,402174.66892918776 201157.51700193272,402174.86288611405 201158.15270021337,402173.6689291877 201158.51700193275,402173.9480438233 201159.43180808862,402173.6689291877 201159.51700193275,402173.960761673 201160.4734713784,402174.9172311187 201160.18163889306,402174.7232682645 201159.5459283156,402175.8755922246 201159.19433782136,402175.96076167305 201159.47347137838,402176.8637731784 201159.19794220856,402176.0537273303 201156.75329463044,402177.0074326843 201156.43747681152,402176.9981326967 201156.41656516813,402176.7105435013 201156.50431273817,402176.63172962464 201156.2459252929,402176.03939897625 201156.50935515473,402175.9981344677 201156.41655202807,402175.7105888302 201156.50428416577,402175.6317365581 201156.24593484204,402175.03939897625 201156.50935515473,402174.99813033856 201156.4165579609,402174.6689291877 201156.51700193272)),((402177.4319188277 201156.2969089858,402182.1684376417 201154.72841926842,402182.0609283978 201154.39738829125,402181.7105963785 201154.50427959274,402181.6317350394 201154.2459276868,402181.03939897625 201154.50935515473,402180.9981302456 201154.4165579893,402180.6689291877 201154.51700193272,402180.8628853744 201155.15269509784,402179.71057818667 201155.5042804746,402179.63173383044 201155.2459282244,402179.0393989762 201155.50935515473,402178.9981307187 201155.416553715,402178.71057979076 201155.50428771586,402178.63173505117 201155.24593237217,402177.74696412834 201155.6394133341,402177.953114895 201156.10300158983,402177.8603038773 201156.14427709693,402177.86287929624 201156.15271863286,402177.79421122815 201156.17367024094,402177.7654533616 201156.18645962258,402177.4319188277 201156.2969089858)),((402183.64391982864 201153.2858719331,402183.6317325998 201153.24592877182,402183.2174463848 201153.43017277692,402183.64391982864 201153.2858719331)),((402182.9981260512 201153.4165592691,402182.66892918776 201153.51700193275,402182.6962441961 201153.60652588095,402183.1093326561 201153.466753958,402183.03207748197 201153.49289388995,402182.9981260512 201153.4165592691)),((402183.1093326561 201153.466753958,402183.2174463848 201153.43017277692,402183.2174463846 201153.43017277698,402183.1093326561 201153.466753958)))',
           "aspect": 16.96769614100002,
           "slope": 25.924800796490043,
           "is_flat": False}

_no_panels = {"roof_plane_id": 2620,
              "roof": "Polygon ((402034.20776647521415725 202521.03231001805397682, 402033.70807106170104817 202520.51486026970087551, 402034.20776647509774193 202520.03231001817039214, 402034.19062131328973919 202520.01455568341771141, 402035.07987398374825716 202519.15581436161301099, 402032.27570524608017877 202516.29223845576052554, 402030.64963580697076395 202517.85775490032392554, 402033.9720082322601229 202521.25997910683508962, 402034.20776647521415725 202521.03231001805397682))",
              "aspect": 10,
              "slope": 224,
              "is_flat": True}


def assert_panel_count(roof, count):
    panels = _roof_panels(
        roof=wkt.loads(roof['roof']),
        panel_w=0.99,
        panel_h=1.64,
        aspect=roof['aspect'],
        slope=roof['slope'],
        panel_spacing_m=0.01,
        is_flat=roof['is_flat'])
    assert len(panels) == count, f"Had {len(panels)}, wanted {count}"


class PanelTest(ParameterisedTestCase):

    def test_specific_failure(self):
        # This failure was caused by the roof plane being a very jagged multipolygon
        # that became invalid once rotated. (it was a multi because the negative
        # buffering from the edge of the building had split it into parts). Fixed by
        # making the algorithm only use the largest sub-polygon of a multi.
        assert_panel_count(_failed, 1)

    def test_small_flat_roof(self):
        assert_panel_count(_no_panels, 1)