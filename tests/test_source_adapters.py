import unittest

from sources import batonrouge, lafayette


class SourceAdapterTests(unittest.TestCase):
    def test_lafayette_location_keeps_cross_street_out_of_city(self):
        street, cross_streets, municipality = lafayette._split_location(
            "3142 AMBASSADOR CAFFERY PKWY/CURRAN PKWY LAFAYETTE, LA"
        )

        self.assertEqual("3142 AMBASSADOR CAFFERY PKWY", street)
        self.assertEqual("CURRAN PKWY", cross_streets)
        self.assertEqual("LAFAYETTE", municipality)

    def test_baton_rouge_intersection_location_is_split_once(self):
        street, cross_streets = batonrouge._split_location(
            "HOOPER RD / SULLIVAN RD",
            "HOOPER RD / SULLIVAN RD",
        )

        self.assertEqual("HOOPER RD", street)
        self.assertEqual("SULLIVAN RD", cross_streets)

    def test_baton_rouge_numbered_address_is_preserved(self):
        street, cross_streets = batonrouge._split_location(
            "14200 S HARRELL'S FERRY RD",
            "WOODBROOK DR / MILLERVILLE RD",
        )

        self.assertEqual("14200 S HARRELL'S FERRY RD", street)
        self.assertEqual("WOODBROOK DR / MILLERVILLE RD", cross_streets)


if __name__ == "__main__":
    unittest.main()
