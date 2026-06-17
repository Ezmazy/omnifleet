import sys
from dwh_watermark import advance_watermark
advance_watermark("dbt", sys.argv[1], -1)
