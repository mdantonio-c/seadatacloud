from restapi.tests import API_URI, BaseTests, FlaskClient


class TestApp(BaseTests):
    def test_01(self, client: FlaskClient) -> None:

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
        r = client.post(f"{API_URI}/orders")
        assert r.status_code == 401

        r = client.get(f"{API_URI}/orders")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/orders")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/orders")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/orders")
        assert r.status_code == 405

        # GET /api/orders/<order_id>/download/<ftype>/c/<code>
        r = client.get(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 500
        assert self.get_content(r) == "Invalid file type my_ftype"

        r = client.post(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.put(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.patch(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405

        r = client.delete(f"{API_URI}/orders/my_order_id/download/my_ftype/c/my_code")
        assert r.status_code == 405
