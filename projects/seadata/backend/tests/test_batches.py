import time

from faker import Faker
from flask import Flask
from restapi.env import Env
from restapi.tests import API_URI, FlaskClient
from tests.custom import SeadataTests


class TestApp(SeadataTests):
    def test_01(self, client: FlaskClient, faker: Faker) -> None:

        # GET /api/ingestion/my_batch_id
        # POST /api/ingestion/my_batch_id
        r = client.get(f"{API_URI}/ingestion/my_batch_id")
        assert r.status_code == 401

        r = client.post(f"{API_URI}/ingestion/my_batch_id")
        assert r.status_code == 401

        r = client.put(f"{API_URI}/ingestion/my_batch_id")
        assert r.status_code == 405
        r = client.delete(f"{API_URI}/ingestion/my_batch_id")
        assert r.status_code == 405
        r = client.patch(f"{API_URI}/ingestion/my_batch_id")
        assert r.status_code == 405

        # DELETE /api/ingestion
        r = client.delete(f"{API_URI}/ingestion")
        assert r.status_code == 401

        r = client.get(f"{API_URI}/ingestion")
        assert r.status_code == 405

        r = client.post(f"{API_URI}/ingestion")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/ingestion")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/ingestion")
        assert r.status_code == 405

        headers = self.login(client)

        # POST - missing parameters
        r = client.post(f"{API_URI}/ingestion/my_batch_id", headers=headers)
        assert r.status_code == 400
        response = self.get_content(r)

        assert isinstance(response, dict)
        self.check_endpoints_input_schema(response)

        r = client.delete(f"{API_URI}/ingestion", headers=headers)
        assert r.status_code == 400
        response = self.get_content(r)

        assert isinstance(response, dict)
        self.check_endpoints_input_schema(response)

        # GET - invalid batch
        r = client.get(f"{API_URI}/ingestion/my_batch_id", headers=headers)
        assert r.status_code == 404

        error = "Batch 'my_batch_id' not enabled or you have no permissions"
        assert self.get_seadata_response(r) == error

        # POST - with no files to be downloaded
        data = self.get_input_data()
        r = client.post(f"{API_URI}/ingestion/my_batch_id", headers=headers, data=data)
        # The request is accepted because no input validation is implemented.
        # The errors will be raised by celery
        assert r.status_code == 200

        # Even if the previous batch request was empty, the batch has been created
        # But the batch can be executed again
        r = client.post(f"{API_URI}/ingestion/my_batch_id", headers=headers, data=data)
        # The request is accepted because no input validation is implemented.
        # The errors will be raised by celery
        assert r.status_code == 200

        # POST - send a valid batch
        batch_id = faker.pystr()
        download_path = "https://github.com/rapydo/http-api/archive/"
        file_name = "v0.6.6.zip"
        file_checksum = "a2b241be6ff941a7c613d2373e10d316"
        file_size = "1473570"
        data_file_count = "1"
        parameters = {
            "backdoor": True,
            "batch_number": batch_id,
            "file_checksum": file_checksum,
            "file_size": file_size,
            "data_file_count": data_file_count,
            "download_path": download_path,
            "file_name": file_name,
        }
        data = self.get_input_data(
            request_id=batch_id,
            api_function="datafiles_download",
            parameters=parameters,
        )

        r = client.post(f"{API_URI}/ingestion/{batch_id}", headers=headers, data=data)
        assert r.status_code == 200

        # GET - valid batch - not_filled yet
        r = client.get(f"{API_URI}/ingestion/{batch_id}", headers=headers)
        assert r.status_code == 200

        content = self.get_seadata_response(r)

        assert isinstance(content, dict)
        assert "batch" in content
        assert "status" in content
        assert "files" in content
        assert content["batch"] == batch_id
        assert content["status"] == "not_filled"
        assert content["files"] == []

        time.sleep(6)

        # GET - valid batch - enabled
        r = client.get(f"{API_URI}/ingestion/{batch_id}", headers=headers)
        assert r.status_code == 200

        content = self.get_seadata_response(r)

        assert isinstance(content, dict)
        assert "batch" in content
        assert "status" in content
        assert "files" in content
        assert content["batch"] == batch_id
        assert content["status"] == "enabled"
        assert isinstance(content["files"], dict)
        assert len(content["files"]) == 1
        assert file_name in content["files"]
        assert isinstance(content["files"][file_name], dict)
        assert "name" in content["files"][file_name]
        assert "path" in content["files"][file_name]
        assert "object_type" in content["files"][file_name]
        assert "owner" in content["files"][file_name]
        assert "content_length" in content["files"][file_name]
        assert "created" in content["files"][file_name]
        assert "last_modified" in content["files"][file_name]
        assert content["files"][file_name]["name"] == file_name
        assert content["files"][file_name]["path"] == f"/tempZone/batches/{batch_id}"
        assert content["files"][file_name]["owner"] == Env.get("IRODS_USER", "")
        assert content["files"][file_name]["object_type"] == "dataobject"
        assert content["files"][file_name]["content_length"] == int(file_size)

        # DELETE BATCH
        parameters = {"batches": [batch_id], "backdoor": True}
        data = self.get_input_data(
            request_id=batch_id, api_function="delete_batch", parameters=parameters
        )

        r = client.delete(f"{API_URI}/ingestion", headers=headers, data=data)
        assert r.status_code == 200

        time.sleep(5)

        # Verify batch is deleted
        r = client.get(f"{API_URI}/ingestion/{batch_id}", headers=headers)
        assert r.status_code == 404

    def test_tasks(self, app: Flask, faker: Faker) -> None:

        pass
        # TO BE IMPLEMENTED
        # batch_path = ""
        # local_path = ""
        # myjson = {}
        # response = self.send_task(
        #     app, "download_batch", batch_path, local_path, myjson
        # )

        # assert response == "Failed"
