from viewer.loader import Loader
from source import *


class TestLoader(Loader):
    buffercache_progress = PSQLSource()
    relfrozenxid_progress = PSQLSource()
    waits = PgStatActivitySource()
    pg_stat_wal_progress = PSQLSource(parse_dates=['ts', 'stats_reset'])
    pg_stat_io_progress = MultiPSQLSource(pivot_col=['backend_type', 'context',
                                                     'object'],
                                          parse_dates=['ts', 'stats_reset'],
                                          plot_values=['writes'])

    pgbench_run_progress = PGBenchRunProgressSource()
    main = MainJSONSource(prefix='')
    iostat = IOStatSource()
    read_exec_reports = ExecutionReportsSource(log_prefix='pgbench_log_read*')



df1 = TestLoader.load('/mnt/force/pgresults/A/tmp.GxDQXTYOYB')
df2 = TestLoader.load('/mnt/force/pgresults/A/tmp.U4pemqW1Eu')
df3 = TestLoader.load('/mnt/force/pgresults/A/tmp.ZSAZK4dScx')
