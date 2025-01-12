from restapi.tests import API_URI, FlaskClient
from tests.custom import SeadataTests


class TestApp(SeadataTests):
    def test_01(self, client: FlaskClient) -> None:

        # POST /api/resourceslist
        r = client.post(f"{API_URI}/resourceslist")
        assert r.status_code == 401

        r = client.get(f"{API_URI}/resourceslist")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/resourceslist")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/resourceslist")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/resourceslist")
        assert r.status_code == 405

        headers = self.login(client)

        r = client.post(f"{API_URI}/resourceslist", headers=headers, json={})
        assert r.status_code == 400
        response = self.get_content(r)

        assert isinstance(response, dict)
        self.check_endpoints_input_schema(response)
