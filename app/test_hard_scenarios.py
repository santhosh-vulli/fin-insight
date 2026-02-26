import unittest
import os
import tempfile
from core.audit import AuditLogger
from datetime import datetime


class TestAuditLoggerHardScenarios(unittest.TestCase):

    def setUp(self):
        # Create temp file safely (Windows compatible)
        temp = tempfile.NamedTemporaryFile(delete=False)
        self.audit_file = temp.name
        temp.close()   # 🔥 VERY IMPORTANT (release file handle on Windows)

        self.logger = AuditLogger(self.audit_file)

    def tearDown(self):
        if os.path.exists(self.audit_file):
            os.remove(self.audit_file)

    def test_corrupt_line_handling(self):
        with open(self.audit_file, 'a') as f:
            f.write('{bad json line}\n')

        self.logger.log_user_action(
            action="test_action",
            description="Valid event",
            user_id="system",
            user_name="System"
        )

        events, corrupt = self.logger._read_events()
        self.assertEqual(len(corrupt), 1)
        self.assertEqual(len(events), 1)

    def test_integrity_verification(self):
        self.logger.log_user_action(
            action="integrity_test",
            description="Testing integrity",
            user_id="system",
            user_name="System"
        )

        with open(self.audit_file, 'r+') as f:
            lines = f.readlines()
            lines[0] = lines[0].replace('"integrity_test"', '"tampered"')
            f.seek(0)
            f.writelines(lines)

        report = self.logger.verify_audit_integrity()
        self.assertEqual(report['integrity_check'], 'FAIL')
        self.assertGreater(report['tampered_events'], 0)

    def test_thread_safe_logging(self):
        import threading
        results = []

        def log_event():
            event = self.logger.log_user_action(
                action="thread_test",
                description="Thread safety",
                user_id="system",
                user_name="System"
            )
            results.append(event)

        threads = [threading.Thread(target=log_event) for _ in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 10)

        events, _ = self.logger._read_events()
        self.assertEqual(len(events), 10)

    def test_generate_audit_report(self):
        for i in range(3):
            self.logger.log_user_action(
                action=f"report_test_{i}",
                description="Report generation",
                user_id="system",
                user_name="System"
            )

        now = datetime.now().isoformat()

        report = self.logger.generate_audit_report(
            start_date="2000-01-01T00:00:00",
            end_date=now,
            report_type="full"
        )

        self.assertEqual(report['total_events'], 3)
        self.assertIn('events', report)


if __name__ == '__main__':
    unittest.main()