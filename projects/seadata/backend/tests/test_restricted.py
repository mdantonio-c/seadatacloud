import time

from faker import Faker
from restapi.env import Env
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

        order_id = faker.pystr()
        # POST - send an empty request
        r = client.post(f"{API_URI}/restricted/my_order_id", headers=headers)
        assert r.status_code == 400

        # POST - send an invalid request (no files to be downloaded)
        data = self.get_input_data(
            request_id=order_id, api_function="download_restricted_order"
        )
        r = client.post(f"{API_URI}/restricted/my_order_id", headers=headers, json=data)
        # The request is accepted because no input validation is implemented.
        # The errors will be raised by celery
        assert r.status_code == 200

        # POST - send a valid order
        download_path = "https://github.com/rapydo/http-api/archive/"
        file_name = "v0.6.6.zip"
        file_checksum = "a2b241be6ff941a7c613d2373e10d316"
        file_size = "1473570"
        data_file_count = "1"
        zipname = f"order_{order_id}_restricted"
        parameters = {
            "backdoor": True,
            "order_number": order_id,
            "zipfile_name": zipname,
            "file_checksum": file_checksum,
            "file_size": file_size,
            "data_file_count": data_file_count,
            "download_path": download_path,
            "file_name": file_name,
        }
        data = self.get_input_data(
            request_id=order_id,
            api_function="download_restricted_order",
            parameters=parameters,
        )

        r = client.post(f"{API_URI}/restricted/{order_id}", headers=headers, json=data)
        assert r.status_code == 200

        # Sending a second restricted order request to verify the merge
        file_name2 = "v0.7.1.zip"
        file_checksum2 = "18c6a99f717bfb9e1416e74f83ac5878"
        file_size2 = "178231"
        data_file_count2 = "1"

        parameters = {
            "backdoor": True,
            "order_number": order_id,
            "zipfile_name": zipname,
            "file_checksum": file_checksum2,
            "file_size": file_size2,
            "data_file_count": data_file_count2,
            "download_path": download_path,
            "file_name": file_name2,
        }
        data = self.get_input_data(
            request_id=order_id,
            api_function="download_restricted_order",
            parameters=parameters,
        )

        r = client.post(f"{API_URI}/restricted/{order_id}", headers=headers, json=data)
        assert r.status_code == 200

        r = client.get(f"{API_URI}/orders/{order_id}", headers=headers, json=data)
        assert r.status_code == 200

        content = self.get_seadata_response(r)

        assert isinstance(content, list)
        # The celery task should still be running...
        assert len(content) == 0

        time.sleep(20)

        r = client.get(f"{API_URI}/orders/{order_id}", headers=headers, json=data)
        assert r.status_code == 200

        content = self.get_seadata_response(r)

        assert isinstance(content, list)
        # The celery task should still be running...
        # two files downloaded, but they are merged in a single zip
        assert len(content) == 1
        assert "name" in content[0]
        assert "path" in content[0]
        assert "object_type" in content[0]
        assert "owner" in content[0]
        assert "content_length" in content[0]
        assert "created" in content[0]
        assert "last_modified" in content[0]
        assert "URL" in content[0]
        assert content[0]["name"] == f"order_{order_id}_restricted.zip"
        assert content[0]["path"] == f"/tempZone/orders/{order_id}"
        assert content[0]["owner"] == Env.get("IRODS_USER", "")
        assert content[0]["object_type"] == "dataobject"
        assert content[0]["content_length"] > int(file_size)
        # should be updated in case of download request
        assert content[0]["URL"] is None
