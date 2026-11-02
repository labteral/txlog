from concurrent.futures import ThreadPoolExecutor, TimeoutError
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from txlog import Call, TxLog


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(prefix='txlog-concurrency-')
        self.addCleanup(self.directory.cleanup)
        self.log = TxLog(self.directory.name)
        self.addCleanup(lambda: self.log._db.close())

    def test_index_reservation_and_write_cannot_overlap(self):
        entered, release, second_started = Event(), Event(), Event()
        original = self.log._put_call

        def pause(index, call):
            if call.method == 'first':
                entered.set()
                self.assertTrue(release.wait(3))
            return original(index, call)

        def append_second():
            second_started.set()
            return self.log.add(Call('second'))

        with patch.object(self.log, '_put_call', side_effect=pause), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.log.add, Call('first'))
            self.assertTrue(entered.wait(2))
            second = pool.submit(append_second)
            try:
                self.assertTrue(second_started.wait(2))
                with self.assertRaises(TimeoutError):
                    second.result(timeout=.05)
            finally:
                release.set()
            self.assertEqual((first.result(timeout=2), second.result(timeout=2)), (0, 1))
        self.log._db.close()
        self.log = TxLog(self.directory.name)
        self.assertEqual([call.method for call in self.log.get_calls()], ['first', 'second'])
        self.assertEqual(self.log._get_index(), 1)

    def test_rollback_does_not_absorb_another_threads_append(self):
        entered, release, other_started = Event(), Event(), Event()

        def transaction():
            self.log.begin()
            try:
                self.log.add(Call('discarded'))
                entered.set()
                self.assertTrue(release.wait(3))
            finally:
                self.log.rollback()

        def append():
            other_started.set()
            return self.log.add(Call('retained'))

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(transaction)
            self.assertTrue(entered.wait(2))
            second = pool.submit(append)
            try:
                self.assertTrue(other_started.wait(2))
                with self.assertRaises(TimeoutError):
                    second.result(timeout=.05)
            finally:
                release.set()
            first.result(timeout=2)
            self.assertEqual(second.result(timeout=2), 0)
        self.assertEqual([call.method for call in self.log.get_calls()], ['retained'])

    def test_replay_serializes_executors_but_allows_new_input_during_a_call(self):
        entered, release = Event(), Event()
        seen = []

        def execute(value):
            seen.append(value)
            if value == 'first':
                entered.set()
                self.assertTrue(release.wait(3))

        self.log.add(Call('execute', ['first']))
        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(self.log.exec_uncommitted_calls, {'execute': execute})
            self.assertTrue(entered.wait(2))
            try:
                self.assertEqual(pool.submit(self.log.add, Call('execute', ['second'])).result(timeout=1), 1)
                other = pool.submit(self.log.exec_uncommitted_calls, {'execute': execute})
                with self.assertRaises(TimeoutError):
                    other.result(timeout=.05)
            finally:
                release.set()
            first.result(timeout=2)
            other.result(timeout=2)
        self.assertEqual(seen, ['first', 'second'])
        self.assertIsNone(self.log.get_first_uncommitted_call())

    def test_failed_write_releases_transaction_ownership(self):
        with patch.object(self.log._db, 'commit', side_effect=OSError('fixture')):
            with self.assertRaises(OSError):
                self.log.add(Call('failed'))
        with ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(self.log.add, Call('retry')).result(timeout=2), 0)
        self.assertEqual(self.log.get(0).method, 'retry')

    def test_commit_batch_advances_each_offset_once(self):
        for number in range(3):
            self.log.add(Call('collect', [number]))
        with self.assertRaises(ValueError):
            self.log.commit_call(self.log.get(1))
        self.log.begin()
        self.log.commit_call(self.log.get(0))
        self.log.commit_call(self.log.get(0))
        self.log.commit_call(self.log.get(1))
        self.log.commit()
        self.assertEqual(self.log.get_first_uncommitted_call().index, 2)
        self.log.commit_call(self.log.get(1))
        self.assertEqual(self.log.get_first_uncommitted_call().index, 2)

    def test_retention_does_not_invalidate_an_existing_reader(self):
        for number in range(3):
            self.log.add(Call('collect', [number]))
        self.log.exec_uncommitted_calls({'collect': lambda value: None})
        reader = self.log.get_calls()
        self.assertEqual(next(reader).index, 0)
        self.log.truncate()
        self.assertEqual([call.index for call in reader], [1, 2])
        self.assertEqual(self.log.count_calls(), 0)

    def test_replay_inside_an_open_batch_fails_without_deadlocking(self):
        self.log.begin()
        try:
            self.log.add(Call('pending'))
            with self.assertRaises(RuntimeError):
                self.log.exec_uncommitted_calls({})
        finally:
            self.log.rollback()
        self.assertEqual(self.log.add(Call('available')), 0)
