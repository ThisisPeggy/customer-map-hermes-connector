import tempfile
import unittest
from pathlib import Path

from connect import _profile_client_id


class ProfileIdentityTests(unittest.TestCase):
    def test_reconnect_reuses_profile_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, ".env").write_text('CUSTOMER_MAP_HERMES_CLIENT_ID="existing-device"\n', encoding="utf-8")
            self.assertEqual(_profile_client_id(directory), "existing-device")
            self.assertNotEqual(_profile_client_id(directory, new_instance=True), "existing-device")

    def test_missing_or_invalid_identity_creates_uuid(self):
        import uuid
        with tempfile.TemporaryDirectory() as directory:
            uuid.UUID(_profile_client_id(directory))
            Path(directory, ".env").write_text("CUSTOMER_MAP_HERMES_CLIENT_ID=invalid device\n", encoding="utf-8")
            uuid.UUID(_profile_client_id(directory))


if __name__ == "__main__":
    unittest.main()
