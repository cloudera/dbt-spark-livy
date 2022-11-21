# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Spark Livy session integration."""

from __future__ import annotations
from cmath import log

import json
import time 
import requests
from urllib import response

import datetime as dt
from types import TracebackType
from typing import Any

import dbt.exceptions
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS

from requests_kerberos import HTTPKerberosAuth

logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)

DEFAULT_POLL_WAIT = 2

class LivySession:
    def __init__(self, connect_url, auth, headers, verify_ssl_certificate):
        self.connect_url = connect_url
        self.auth = auth
        self.headers = headers
        self.session_id = None
        self.verify_ssl_certificate = verify_ssl_certificate

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.delete_session()
        return True

    def create_session(self, data):
        # Create sessions
        response = None
        try:
            response = requests.post(
                       self.connect_url + '/sessions', data=json.dumps(data),
                       headers=self.headers,
                       auth=self.auth,
                       verify=self.verify_ssl_certificate
            )
            response.raise_for_status()
        except requests.exceptions.ConnectionError as c_err:
            print("Connection Error :", c_err)
        except requests.exceptions.HTTPError as h_err:
            print("Http Error: ", h_err)
        except requests.exceptions.Timeout as t_err:
            print("Timeout Error: ", t_err)
        except requests.exceptions.RequestException as a_err:
            print("Authorization Error: ", a_err)

        if response is None:
            raise Exception("Invalid response from livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()['id'])
        except requests.exceptions.JSONDecodeError as json_err:
            raise Exception("Json decode error to get session_id") from json_err

        # Wait for started state
        while True:
            res = requests.get(
                self.connect_url + '/sessions/' + self.session_id + '/state',
                headers=self.headers,
                auth=self.auth,
                verify=self.verify_ssl_certificate
            ).json()

            if res['state'] == 'idle':
                break
            if res['state'] == 'dead':
                print("ERROR, cannot create a livy interactive session")
                raise dbt.exceptions.FailedToConnectException(
                        'failed to connect'
                    )
                return

            time.sleep(DEFAULT_POLL_WAIT)

        logger.debug(f"Creating new livy session: {self.session_id}")

        return self.session_id

    def delete_session(self):
        logger.debug(f"Closing the livy session: {self.session_id}")

        try:
            # delete the session_id
            _ = requests.delete(
                self.connect_url + '/sessions/' + self.session_id,
                headers=self.headers,
                auth=self.auth,
                verify=self.verify_ssl_certificate
            )
        except Exception as ex:
            logger.error(f"Unable to close the livy session {self.session_id}, error: {ex}")

    def is_valid_session(self):
        res = requests.get(
                self.connect_url + '/sessions/' + self.session_id + '/state',
                headers=self.headers,
                auth=self.auth,
                verify=self.verify_ssl_certificate
            ).json()

        return res['state'] == 'idle'


# cursor object - wrapped for livy API
class LivyCursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self) -> None:
        self._schema = None
        self._rows = None
        self.session_id = -1
        self.auth = None
        self.headers = None

    def __init__(self, connect_url, session_id, auth, headers, verify_ssl_certificate) -> None:
        self._rows = None
        self._schema = None
        self.connect_url = connect_url
        self.session_id = session_id
        self.auth = auth
        self.headers = headers
        self.verify_ssl_certificate = verify_ssl_certificate

    def __enter__(self) -> LivyCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        """
        Get the description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field['name'],
                    field['type'], # field['dataType'],
                    None,
                    None,
                    None,
                    None,
                    field['nullable'],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._rows = None
        
    def _submitLivyCode(self, code):
        # Submit code
        data = {'code': code}

        res = requests.post(self.connect_url + '/sessions/' + self.session_id + '/statements', 
              data=json.dumps(data), 
              headers=self.headers, 
              auth=self.auth, 
              verify=self.verify_ssl_certificate
        )

        return res


    def _getLivySQL(self, sql):
        # Comment, what is going on?!
        # The following code is actually injecting SQL to pyspark object for executing it via the Livy session - over an HTTP post request.
        # Basically, it is like code inside a code. As a result the strings passed here in 'escapedSQL' variable are unescapted and interpreted on the server side. 
        # This may have repurcursions of code injection not only as SQL, but also arbritary Python code. An alternate way safer way to acheive this is still unknown. 
        # escapedSQL = sql.replace("\n", "\\n").replace('"', '\\\"')
        # code = "val sprk_sql = spark.sql(\"" + escapedSQL + "\")\nval sprk_res=sprk_sql.collect\n%json sprk_res"  # .format(escapedSQL)

        # TODO: since the above code is not changed to sending direct SQL to the livy backend, client side string escaping is probably not needed
        code = sql

        # print(code)

        return code

    def _getLivyResult(self, res_obj):
        json_res = res_obj.json()

        while True:
            res = requests.get(
                  self.connect_url + '/sessions/' + self.session_id + '/statements/' + repr(json_res['id']), 
                  headers=self.headers, 
                  auth=self.auth,
                  verify=self.verify_ssl_certificate
            ).json()

            # print(res)

            if res['state'] == 'available':
                return res
            time.sleep(DEFAULT_POLL_WAIT)

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters
        
        # TODO: handle parameterised sql

        res = self._getLivyResult(self._submitLivyCode(self._getLivySQL(sql)))
        
        if (res['output']['status'] == 'ok'):
            # values = res['output']['data']['application/json']
            values = res['output']['data']['application/json']
            if (len(values) >= 1):
                self._rows = values['data'] # values[0]['values']
                self._schema = values['schema']['fields'] # values[0]['schema']
                # print("rows", self._rows)
                # print("schema", self._schema)
            else:
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise dbt.exceptions.raise_database_error(
                        'Error while executing query: ' + res['output']['evalue']
                    ) 

    def fetchall(self):
        """
        Fetch all data.

        Returns
        -------
        out : list() | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self):
        """
        Fetch the first output.

        Returns
        -------
        out : one row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
       
        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row

class LivyConnection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(self, connect_url, session_id, auth, headers, session_params, verify_ssl_certificate) -> None:
        self.connect_url = connect_url
        self.session_id = session_id
        self.auth = auth
        self.headers = headers
        self.session_params = session_params
        self.verify_ssl_certificate = verify_ssl_certificate

        self._cursor = LivyCursor(self.connect_url, self.session_id, self.auth, self.headers, self.verify_ssl_certificate)

    def get_session_id(self):
        return self.session_id

    def get_auth(self):
        return self.auth

    def get_headers(self):
        return self.headers

    def get_connect_url(self):
        return self.connect_url

    def cursor(self):
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

class LivyConnectionManager:

    def __init__(self):
        self.livy_global_session = None

    def connect(self, connect_url, user, password, auth_type, session_params, verify_ssl_certificate):
        if auth_type and auth_type.lower() == "kerberos":
            logger.debug("Using Kerberos auth")
            auth = HTTPKerberosAuth()
        else:
            logger.debug("Using HTTP auth")
            auth = requests.auth.HTTPBasicAuth(user, password)

        # the following opens an spark / sql session
        data = {
            'kind': 'sql', # 'spark'
            'conf': session_params
        }

        headers = {
            'Content-Type': 'application/json'
        }

        if (self.livy_global_session == None):
            self.livy_global_session = LivySession(connect_url, auth, headers, verify_ssl_certificate)
            self.livy_global_session.create_session(data)
        elif not self.livy_global_session.is_valid_session():
            self.livy_global_session.delete_session()
            self.livy_global_session.create_session(data)
        else:
            logger.debug(f"Reusing session: {self.livy_global_session.session_id}")

        livyConnection = LivyConnection(
                            connect_url,
                            self.livy_global_session.session_id,
                            auth,
                            headers,
                            session_params,
                            verify_ssl_certificate
                        )

        return livyConnection

class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        logger.debug("NotImplemented: cancel")

    def close(self):
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value == None:
            return "''"
        else:
            return f"'{value}'"

