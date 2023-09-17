from viewer.loader import Loader
from source import *


class TestLoader(Loader):
    buffercache_progress = PSQLSource()
    relfrozenxid_progress = PSQLSource()
    aggwaits = PSQLSource()
    pg_stat_wal_progress = PSQLSource(parse_dates=['ts', 'stats_reset'])
    # pg_stat_io_progress = PSQLSource(parse_dates=['ts', 'stats_reset'])

    pgbench_run_progress = PGBenchRunProgressSource()
    main = MainJSONSource()
    iostat = IOStatSource()


df = TestLoader.load('/mnt/force/pgresults/A/tmp.84G0vTOri4')

print(df)
print("Index:", df.index.dtype)
print(df.dtypes)
