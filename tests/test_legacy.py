"""Roundtrip the unchanged 2.x key layout and Python Call records."""
import pickle
from tempfile import TemporaryDirectory
import unittest

from txlog import Call, TxLog


class LegacyTest(unittest.TestCase):
    def test_legacy_calls_replay_rollback_and_reopen(self):
        with TemporaryDirectory() as directory:
            log = TxLog(directory)
            try:
                call = Call('collect', [1])
                call._index = 0
                log._db.db.put(b'\x00' + (0).to_bytes(5, 'big'),
                               pickle.dumps(call, protocol=5))
                log._db.put(b'\x03index', 0)
                log.begin()
                log.add(Call('collect', [2]))
                log.rollback()
                self.assertEqual(log.count_calls(), 1)
                seen = []
                log.exec_uncommitted_calls({'collect': seen.append})
                self.assertEqual(seen, [1])
                log._db.reload()
                self.assertTrue(log.get(0).committed)
                log.add(Call('collect', [3]))
                log.exec_uncommitted_calls({'collect': seen.append})
                self.assertEqual(seen, [1, 3])
            finally:
                log._db.close()
