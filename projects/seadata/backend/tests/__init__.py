import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast

from restapi.env import Env
from restapi.tests import AUTH_URI, BaseTests, FlaskClient
from werkzeug.test import TestResponse as Response

USER = Env.get("AUTH_DEFAULT_USERNAME", "")
PASSWORD = Env.get("AUTH_DEFAULT_PASSWORD", "")


class SeadataTests(BaseTests):
    def get_seadata_response(
        self, http_out: Response
    ) -> Union[str, List[Any], Dict[str, Any]]:

        response = self.get_content(http_out)
        assert isinstance(response, dict)
        assert "Response" in response
        assert "data" in response["Response"]

        return cast(
            Dict[str, Any],
            response["Response"]["data"],
        )

    def login(self, client: FlaskClient) -> Dict[str, str]:

        r = client.post(
            f"{AUTH_URI}/seadata/login",
            json={"username": USER, "password": PASSWORD},
        )

        assert r.status_code == 200
        data = self.get_seadata_response(r)
        assert isinstance(data, dict)
        assert "token" in data

        token = data["token"]
        assert token is not None

        return {"Authorization": f"Bearer {token}"}

    def check_endpoints_input_schema(self, response: Dict[str, Any]) -> None:
        assert "api_function" in response
        assert "Missing data for required field." in response["api_function"]

        assert "datetime" in response
        assert "Missing data for required field." in response["datetime"]

        assert "edmo_code" in response
        assert "Missing data for required field." in response["edmo_code"]

        assert "parameters" in response
        assert "Missing data for required field." in response["parameters"]

        assert "request_id" in response
        assert "Missing data for required field." in response["request_id"]

        assert "test_mode" in response
        assert "Missing data for required field." in response["test_mode"]

        assert "version" in response
        assert "Missing data for required field." in response["version"]

    def get_input_data(
        self,
        request_id: str = "my_request_id",
        api_function: str = "my_api_function",
        parameters: Optional[Any] = None,
    ) -> Dict[str, Union[bool, int, str]]:
        if parameters is None:
            parameters = {}
        parameters["backdoor"] = True

        data: Dict[str, Union[bool, int, str]] = {
            "api_function": api_function,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%s"),
            "edmo_code": 1234,
            "parameters": json.dumps(parameters),
            "request_id": request_id,
            "test_mode": "1",
            "version": "1",
            "eudat_backdoor": True,
        }
        return data
