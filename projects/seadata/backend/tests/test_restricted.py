from faker import Faker
from restapi.tests import API_URI, FlaskClient
from tests.custom import SeadataTests


class TestApp(SeadataTests):
    def test_01(self, client: FlaskClient, faker: Faker) -> None:

        # POST /api/restricted/<order_id>
        r = client.post(f"{API_URI}/restricted/my_order_id")
        assert r.status_code == 401

        r = client.get(f"{API_URI}/restricted/my_order_id")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/restricted/my_order_id")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/restricted/my_order_id")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/restricted/my_order_id")
        assert r.status_code == 405

        headers = self.login(client)

        r = client.post(f"{API_URI}/restricted/my_order_id", headers=headers)
        # Default irods user is not an admin and not allowed to send restricted orders
        assert r.status_code == 401

        # r = client.post(f"{API_URI}/restricted/my_order_id", headers=headers)
        # assert r.status_code == 400
        # response = self.get_content(r)

        # assert isinstance(response, dict)
        # self.check_endpoints_input_schema(response)

        # order_id = faker.pystr()
        # download_path = "https://github.com/rapydo/http-api/archive/"
        # file_name = "v0.6.6.zip"
        # file_checksum = "a2b241be6ff941a7c613d2373e10d316"
        # file_size = "1473570"
        # data_file_count = "1"
        # zipname = f"order_{order_id}_restricted"
        # params = {
        #     "request_id": order_id, "edmo_code": 634, "datetime": now,
        #     "version": "1", "api_function": "download_restricted_order",
        #     "test_mode": "true", "parameters": {
        #         "order_number": order_id,
        #         "backdoor": True,
        #         "zipfile_name": zipname,

        #         "file_checksum": file_checksum,
        #         "file_size": file_size,
        #         "data_file_count": data_file_count,
        #         "download_path": download_path,
        #         "file_name": file_name
        #     }
        # }

        # apiclient.call(
        #     URI, method='post', endpoint='/api/restricted/%s' % order_id,
        #     token=token, payload=params
        # )

        # print_section("Sending a second restricted order request...")
        # params = {
        #     "request_id": order_id, "edmo_code": 634, "datetime": now,
        #     "version": "1", "api_function": "download_restricted_order",
        #     "test_mode": "true", "parameters": {
        #         "order_number": order_id,
        #         "backdoor": True,
        #         "zipfile_name": zipname,

        #         "file_checksum": file_checksum2,
        #         "file_size": file_size2,
        #         "data_file_count": data_file_count2,
        #         "download_path": download_path,
        #         "file_name": file_name2
        #     }
        # }
