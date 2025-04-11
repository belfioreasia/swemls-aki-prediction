import unittest
from unittest.mock import patch, MagicMock
from prometheus_client import CollectorRegistry
from messages_manager import MessagesManager

class TestMLLPConnection(unittest.TestCase):

    @patch('socket.create_connection')
    def test_connection_retry_success(self, mock_create_connection):
        """Test successful reconnection after initial failures."""
        mock_create_connection.side_effect = [ConnectionRefusedError, ConnectionRefusedError, MagicMock()]
        registry = CollectorRegistry()

        manager = MessagesManager(mllp_address='localhost', mllp_port=8440, max_retries=5, registry=registry)
        manager.connect_to_mllp()

        self.assertEqual(mock_create_connection.call_count, 3)
        self.assertIsNotNone(manager.socket)

    @patch('socket.create_connection', side_effect=ConnectionRefusedError)
    def test_connection_retry_failure(self, mock_create_connection):
        """Test system behavior when connection fails after all retries."""
        registry = CollectorRegistry()
        manager = MessagesManager(mllp_address='localhost', mllp_port=8440, max_retries=3, registry=registry)
        manager.connect_to_mllp()

        self.assertEqual(mock_create_connection.call_count, 3)
        self.assertIsNone(manager.socket)

if __name__ == '__main__':
    unittest.main()