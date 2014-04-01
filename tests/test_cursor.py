import unittest
import os.path
import shutil
from datetime import datetime, timedelta
from handbag import database

TEST_PATH = "/tmp/handbag-test.db"
TEST_URL = "lmdb://%s" % TEST_PATH


class TestCursor(unittest.TestCase):
    
    def setUp(self):
        if os.path.exists(TEST_PATH):
            shutil.rmtree(TEST_PATH)
        self.db = database.open(TEST_URL)
        self.table = self.db.foo
        self.empty_table = self.db.bar
        self.records = []
        
        with self.db.write():
            for i in range(0,20):
                rec = {
                    'id': '%.2d' % i,
                    'value': "foo.%d" % i
                }
                self.table.save(rec)
                self.records.append(rec)
                
                
    def test_first(self):
        with self.db.read():
            cur = self.table.cursor()
            doc = cur.first()
            self.assertEqual(doc, {'id':'00', 'value':'foo.0'})
            
            
    def test_last(self):
        with self.db.read():
            cur = self.table.cursor()
            doc = cur.last()
            self.assertEqual(doc, {'id':'19', 'value':'foo.19'})
            
            
    def test_all(self):
        with self.db.read():
            records = list(self.table.cursor())
            self.assertEqual(records, self.records)
            
            
    def test_range(self):
        with self.db.read():
            records = list(self.table.cursor().range('03','07'))
            self.assertEqual(records, self.records[3:7])
            
    
    def test_out_of_range(self):
        with self.db.read():
            records = list(self.table.cursor().range('30'))
            self.assertEqual(records, [])
            
            
    def test_date_range(self):
        table = self.empty_table
        now = datetime.now()
        before_now = now - timedelta(days=1)
        after_now = now + timedelta(days=1)
        way_after_now = now + timedelta(days=30)
        
        with self.db.write():
            table.save({ 'id': before_now })
            table.save({ 'id': after_now })
        
        with self.db.read():
            results = list(table.cursor().range(start=now))
            self.assertEqual(len(results), 1)
            results = list(table.cursor().range(start=way_after_now))
            self.assertEqual(len(results), 0)
            
            
    def test_prefix(self):
        with self.db.read():
            records = list(self.table.cursor().prefix('0'))
            self.assertEqual(records, self.records[:10])
            
            
    def test_key(self):
        with self.db.read():
            records = list(self.table.cursor().key('04'))
            self.assertEqual(records, self.records[4:5])
            
            
    def test_first_reverse(self):
        with self.db.read():
            cur = self.table.cursor(reverse=True)
            doc = cur.first()
            self.assertEqual(doc, {'id':'19', 'value':'foo.19'})
            
            
    def test_last_reverse(self):
        with self.db.read():
            cur = self.table.cursor(reverse=True)
            doc = cur.last()
            self.assertEqual(doc, {'id':'00', 'value':'foo.0'})
            
            
    def test_all_reverse(self):
        with self.db.read():
            records = list(self.table.cursor(reverse=True))
            self.assertEqual(records, list(reversed(self.records)))
            
            
    def test_range_reverse(self):
        with self.db.read():
            records = list(self.table.cursor(reverse=True).range('03','07'))
            self.assertEqual(records, list(reversed(self.records[3:7])))
            
            
    def test_prefix_reverse(self):
        with self.db.read():
            records = list(self.table.cursor(reverse=True).prefix('1'))
            self.assertEqual(records, list(reversed(self.records[10:])))
            
            
    def test_key_reverse(self):
        with self.db.read():
            records = list(self.table.cursor(reverse=True).key('04'))
            self.assertEqual(records, self.records[4:5])
            
            
    def test_random(self):
        with self.db.read():
            records = list(self.table.cursor().random())
            self.assertNotEqual(records, self.records)
            