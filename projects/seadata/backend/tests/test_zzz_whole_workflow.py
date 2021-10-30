import time

from faker import Faker
from restapi.tests import API_URI, FlaskClient
from tests.custom import SeadataTests


class TestApp(SeadataTests):
    def test_01(self, client: FlaskClient, faker: Faker) -> None:

        time.sleep(1)
        API_URI

        # 1 . create a batch
        # 2 . approve the batch
        # 3 . create an order
        # 4 . create a restricted order
        # 5 . download the order
        # 6 . delete the batch
        # 7 . delete the order
