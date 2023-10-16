# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import logging
import time
from io import IOBase
from typing import Optional, Union
from collections.abc import Sequence
from requests_toolbelt import MultipartEncoder

import requests
from flask_babel import gettext as __

from superset import app
from superset.reports.models import ReportRecipientType
from superset.reports.notifications.base import BaseNotification
from superset.reports.notifications.exceptions import NotificationError
from retry import retry

logger = logging.getLogger(__name__)


class LarkNotification(BaseNotification):  # pylint: disable=too-few-public-methods
    """
    Sends a lark notification for a report recipient
    """

    type = ReportRecipientType.LARK

    def _request_lark_api(self, url: str, data) -> Optional[dict]:
        logger.info("lark api request {url: %s, data: %s }", url, json.dumps(data))
        r = requests.post(url,
                          headers={"Content-Type": "application/json"},
                          data=json.dumps(data))
        result = r.json()
        print(r)
        logger.info("lark api status_code:%d, response: %s", r.status_code, result)
        return result

    def _get_webhook_url(self) -> str:
        return json.loads(self._recipient.recipient_config_json)["target"]

    def _get_tenant_access_token(self, app_id: str, app_secret: str) -> str:
        result = self._request_lark_api(
            "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
            {
                "app_id": app_id,
                "app_secret": app_secret
            })
        if result is not None and 'tenant_access_token' in result.keys():
            return result["tenant_access_token"]

    @retry(requests.HTTPError, delay=5, backoff=1, tries=5)
    def _files_upload(self, tenant_access_token: str, files: Sequence[bytes]) -> str:
        try:
            for i,file in enumerate(files):
                img_path = "/tmp/" + self._content.name+ str(i) + ".png"
                logger.info("img path: %s",img_path)
                with open(img_path, 'wb') as f:
                    f.write(file)

                url = "https://open.larksuite.com/open-apis/im/v1/images"
                form = {'image_type': 'message',
                        'image': (open(img_path, 'rb'))}
                multi_form = MultipartEncoder(form)
                headers = {
                    "Authorization": "Bearer %s" % tenant_access_token
                }
                headers['Content-Type'] = multi_form.content_type
                r = requests.request("POST", url, headers=headers, data=multi_form)
                print(r.content)

            r.raise_for_status()
            result = r.json()
            print(r)
            logger.info("lark api status_code:%d, response: %s",
                        r.status_code, result)
            if result is not None:
                if result["code"] == 0:
                    return result["data"]["image_key"]
                elif result['code'] == 40010:
                    logger.error("Upload image error, response body=>%s", json.dumps(result))
                    raise requests.HTTPError("Upload image error,error code:40010,"
                                            "response: %s" % json.dumps(result))
                else:
                    raise Exception(
                        "Upload image error,response code:%d,response body: %s" %
                        (r.status_code, json.dumps(result)))
            else:
                raise Exception("Http request error,response code: %d,text: %s",
                                r.status_code, r.text)
        except Exception as ex:
            logger.error
            logger.error(r.request.path_url)
            raise ex

    @staticmethod
    def _error_template(name: str, description: str, text: str) -> str:
        return __(
            """
            *%(name)s*\n
            %(description)s\n
            Error: %(text)s
            """,
            name=name,
            description=description,
            text=text,
        )

    def _get_body(self, img_key=None) -> dict:
        if self._content.text:
            return json.loads(
                """
                {
                    "msg_type": "%(name)s",
                    "content": {
                    "text": "%(description)s\\nError: %(text)s"
                    }
                }
                """ % {"name": self._content.name,
                       "description": self._content.description or "",
                       "text": self._content.text}
            )

        if img_key:
            return json.loads(
                """
        {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": true
            },
            "i18n_elements": {
                "zh_cn": [
                    {
                        "tag": "markdown",
                        "content": "**%(name)s**\\n %(now)s\\n[%(title)s](%(url)s)\\n"
                    },
                    {
                        "tag": "img",
                        "title": {
                            "tag": "lark_md",
                            "content": " %(description)s"
                        },
                        "img_key": "%(img_key)s",
                        "alt": {
                            "tag": "plain_text",
                            "content": "%(description)s"
                        }
                    }
                ]
            }
        }
        }
                """ % {
                    "name": self._content.name,
                    "title": "Explore in Superset",
                    "description": self._content.description or "",
                    "now": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    "url": self._content.url,
                    "img_key": img_key,
                }
            )
        else:
            return json.loads(
                """
                {
    "msg_type": "post",
    "content":
    {
        "post":
        {
            "zh_cn":
            {
                "title": "%(name)s",
                "content":
                [
                    [
                        {
                            "tag": "text",
                            "text": "%(description)s"
                        }
                    ]
                ]
            }
        }
    }
}
                """ % {"name": self._content.name,
                       "description": self._content.description or "", }
            )

    def _get_inline_file(self) -> Sequence[Union[str, IOBase, bytes]]:
        if self._content.csv:
            return [self._content.csv]
        if self._content.screenshots:
            return self._content.screenshots
        return []

    @retry(IOError, delay=10, backoff=2, tries=5)
    def send(self) -> None:
        files = self._get_inline_file()
        try:
            app_id = app.config["APP_ID"]
            app_secret = app.config["APP_SECRET"]
            lark_webhook_url = self._get_webhook_url()
            tenant_access_token = self._get_tenant_access_token(app_id, app_secret)
            logger.info("tenant_access_token: %s", tenant_access_token)

            if len(files) != 0:
                # Upload file to lark server
                logger.info("Uploading file to lark server...")
                image_key = self._files_upload(tenant_access_token, files)
                logger.info("image_key: %s", image_key)
                payload = self._get_body(image_key)
                # Send lark message
                logger.info("Sending report to lark...")
                logger.info("Lark message is: %s", payload)
                result = self._request_lark_api(lark_webhook_url,
                                                data=payload)
                logger.info("response:%s", json.dumps(result))
            else:
                payload = self._get_body()
                self._request_lark_api(lark_webhook_url,
                                       data=payload)
                logger.info("Report sent to lark")
        except IOError as ex:
            raise NotificationError(ex)

