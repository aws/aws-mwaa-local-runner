import unittest
import unittest.mock

from plugins import snowflake_connector


class TestSnowflakeHook(unittest.TestCase):
    def setUp(self):
        self.snowflake_secrets_value = {'SecretString':'{"user":"Harry","account":"hogwartsacct","private_key_passphrase":"I solemnly swear that i am up to no good ", "private_key_encrypted":"mischief managed"}'}
        self.TDM_NAME = "tdm_bogus"
        self.TDM_SHORT_NAME = "bogus"
        self.SECRETS_MANAGER_KEY = "tdm/tdm_bogus_prod_etl_srvc"
        #self.local_connection_config = {"account": "sdm","database": "TDM_SANDBOX","host": "host.docker.internal","password": "<password>","port": "1444","protocol": "http","region": 'us-west-2',"schema": "SANDBOX","user": "<username>","warehouse": "TDM_SANDBOX"}

    @unittest.mock.patch('plugins.snowflake_connector.boto3')
    def test_get_boto_client(self, mock_boto3):
        mock_secrets_manager_client = unittest.mock.MagicMock()
        mock_boto3.client.side_effect = [mock_secrets_manager_client]

        snowflake_connector.get_boto_client(service_name='secretsmanager')

        print(mock_boto3.method_calls)
        print(mock_boto3.mock_calls)
        print(mock_secrets_manager_client.method_calls)
        print(mock_secrets_manager_client.mock_calls)
        mock_boto3.client.assert_called_once()

    def test_get_boto_client_context_manager(self):
        with unittest.mock.patch('plugins.snowflake_connector.boto3') as mock_boto3:
            snowflake_connector.get_boto_client(service_name='secretsmanager')
            mock_boto3.client.assert_called_once()

    @unittest.mock.patch('plugins.snowflake_connector.snowflake.connector')
    @unittest.mock.patch('plugins.snowflake_connector.crypto_serialization')
    @unittest.mock.patch('plugins.snowflake_connector.crypto_default_backend')
    @unittest.mock.patch('plugins.snowflake_connector.boto3')
    @unittest.mock.patch("plugins.snowflake_connector.TDM_NAME", "tdm_bogus")
    @unittest.mock.patch("plugins.snowflake_connector.TDM_SHORT_NAME", "bogus")
    @unittest.mock.patch("plugins.snowflake_connector.SECRETS_MANAGER_KEY", "tdm/tdm_bogus_prod_etl_srvc")
    def test_get_conn(self, mock_boto, mock_crypto_serialization, mock_crypto_default_backend, mock_snowflake):
        # setup
        mock_secrets_manager_client = unittest.mock.MagicMock()
        mock_secrets_manager_client.get_secret_value.return_value = self.snowflake_secrets_value
        mock_boto.client.side_effect = [mock_secrets_manager_client]

        # test
        operator = snowflake_connector.SnowflakeConnectorOperator(task_id='dummyTask')
        operator.get_conn()

        # assertions
        mock_secrets_manager_client.get_secret_value.assert_called_once()
        mock_snowflake.connect.assert_called_once()
        mock_crypto_serialization.assert_called_once()
        mock_crypto_default_backend.load_pem_private_key.assert_called()

    def test_local_snowflake_connection(self):
        result = snowflake_connector.get_local_snowflake_connection()
        self.assertEqual(result['account'], "sdm")
        self.assertFalse('True')

    @unittest.mock.patch('plugins.snowflake_connector.crypto_serialization')
    @unittest.mock.patch('plugins.snowflake_connector.crypto_default_backend')
    @unittest.mock.patch('plugins.snowflake_connector.boto3')
    @unittest.mock.patch("plugins.snowflake_connector.TDM_NAME", "tdm_bogus")
    @unittest.mock.patch("plugins.snowflake_connector.TDM_SHORT_NAME", "bogus")
    @unittest.mock.patch("plugins.snowflake_connector.SECRETS_MANAGER_KEY", "tdm/tdm_bogus_prod_etl_srvc")
    def test_get_snowflake_connection_with_secrets_manager(self,
                                                           mock_boto,
                                                           mock_crypto_serialization,
                                                           mock_crypto_default_backend):
        # setup
        mock_secrets_manager_client = unittest.mock.MagicMock()
        mock_secrets_manager_client.get_secret_value.return_value = self.snowflake_secrets_value
        mock_boto.client.side_effect = [mock_secrets_manager_client]

        # test
        snowflake_connector.get_snowflake_connection_with_secrets_manager(
            credentials_client=mock_secrets_manager_client)

        # assertions
        mock_secrets_manager_client.get_secret_value.assert_called_once()
        mock_crypto_serialization.assert_called()
        mock_crypto_default_backend.load_pem_private_key.assert_called()
