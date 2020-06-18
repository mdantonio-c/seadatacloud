# -*- coding: utf-8 -*-

from seadata.tasks.seadata import ext_api, celery_app
from seadata.tasks.seadata import notify_error
from seadata.apis.commons.seadatacloud import ErrorCodes

from restapi.connectors.celery import send_errors_by_email
from restapi.utilities.logs import log


@celery_app.task(bind=True)
@send_errors_by_email
def list_resources(self, batch_path, order_path, myjson):

    with celery_app.app.app_context():

        try:
            with celery_app.get_service(service='irods') as imain:

                param_key = 'parameters'

                if param_key not in myjson:
                    myjson[param_key] = {}

                myjson[param_key]['request_id'] = myjson['request_id']
                myjson['request_id'] = self.request.id

                params = myjson.get(param_key, {})
                backdoor = params.pop('backdoor', False)

                if param_key not in myjson:
                    myjson[param_key] = {}

                myjson[param_key]['batches'] = []
                batches = imain.list(batch_path)
                for n in batches:
                    myjson[param_key]['batches'].append(n)

                myjson[param_key]['orders'] = []
                orders = imain.list(order_path)
                for n in orders:
                    myjson[param_key]['orders'].append(n)

                ret = ext_api.post(myjson, backdoor=backdoor)
                log.info('CDI IM CALL = {}', ret)

                return "COMPLETED"
        except BaseException as e:
            log.error(e)
            log.error(type(e))
            return notify_error(
                ErrorCodes.UNEXPECTED_ERROR,
                myjson, backdoor, self
            )