from faker import Faker
from restapi.tests import API_URI, FlaskClient
from tests.custom import SeadataTests


class TestApp(SeadataTests):
    def test_01(self, client: FlaskClient, faker: Faker) -> None:

        # GET /api/orders/my_order_id
        # PUT /api/orders/my_order_id
        r = client.get(f"{API_URI}/orders/my_order_id")
        assert r.status_code == 401

        r = client.put(f"{API_URI}/orders/my_order_id")
        assert r.status_code == 401

        r = client.post(f"{API_URI}/orders/my_order_id")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/orders/my_order_id")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/orders/my_order_id")
        assert r.status_code == 405

        # POST /api/orders
        # DELETE /api/orders
        r = client.post(f"{API_URI}/orders")
        assert r.status_code == 401

        r = client.delete(f"{API_URI}/orders")
        assert r.status_code == 401

        r = client.get(f"{API_URI}/orders")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/orders")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/orders")
        assert r.status_code == 405

        # GET /api/orders/<order_id>/download/<ftype>/c/<code>
        r = client.get(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 400
        assert self.get_seadata_response(r) == "Invalid file type my_ftype"

        r = client.post(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        headers = self.login(client)

        r = client.post(f"{API_URI}/orders", headers=headers, json={})
        assert r.status_code == 400
        response = self.get_content(r)

        assert isinstance(response, dict)
        self.check_endpoints_input_schema(response)

        r = client.delete(f"{API_URI}/orders", headers=headers, json={})
        assert r.status_code == 400
        response = self.get_content(r)

        assert isinstance(response, dict)
        self.check_endpoints_input_schema(response)

        # Test download with wrong ftype (only accepts 0x and 1x as types)
        r = client.get(f"{API_URI}/orders/my_order_id/download/0/c/my_code")
        assert r.status_code == 400
        assert self.get_seadata_response(r) == "Invalid file type 0"

        # Test download with wrong ftype (only accepts 0x and 1x as types)
        r = client.get(f"{API_URI}/orders/my_order_id/download/20/c/my_code")
        assert r.status_code == 400
        assert self.get_seadata_response(r) == "Invalid file type 20"

        # Test download with wrong code (ftype 00 == unrestricted orders)
        r = client.get(f"{API_URI}/orders/my_order_id/download/00/c/my_code")
        assert r.status_code == 404
        error = "Order 'my_order_id' not found (or no permissions)"
        assert self.get_seadata_response(r) == error

        # order_id = faker.pystr()
        # pids = ["00.T12345/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"]
        # params = {
        #     "request_id": order_id, "edmo_code": 12345, "datetime": now,
        #     "version": "1", "api_function": "order_create_zipfile",
        #     "test_mode": "true", "parameters": {
        #         "backdoor": True,
        #         "login_code": "unknown", "restricted": "false",
        #         "file_name": f"order_{order_id}_unrestricted",
        #         "order_number": order_id, "pids": pids, "file_count": len(pids),
        #     }
        # }

        # apiclient.call(
        #     URI, method='post', endpoint='/api/orders',
        #     token=token, payload=params
        # )

        # print_section("Request download links")
        # # PUT /api/order/<OID> -> return iticket_code
        # out = apiclient.call(
        #     URI, method='put', endpoint='/api/orders/%s' % order_id,
        #     token=token
        # )

        # # DELETE ORDER
        # params = {
        #     "request_id": order_id, "edmo_code": 634, "datetime": now,
        #     "version": "1", "api_function": "delete_orders",
        #     "test_mode": "true", "parameters": {
        #         "orders": [order_id],
        #         "backdoor": True
        #     }
        # }

        # apiclient.call(
        #     URI, method='delete', endpoint='/api/orders',
        #     token=token, payload=params
        # )
