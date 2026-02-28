# Argo workflows

## Client test

Will create and then populate a DB, S3 objects to load wikipedia page articles from a given archive.

    argo submit awfs/mwpam-etl-full.yaml -p drop_db=1

mwpam-etl-htdss-sfe-svr-2572231168: 2026-02-27 13:23:05 sfe_svr INFO     main: listen 0.0.0.0:8080 DB t-db-pgs p11_mwpam_full

mwpam-etl-htdss-mwpam-map-etl-reduce-980289887: 2026-02-27 13:38:08 mw.pam.reduce INFO     do_run: rpps {'pack_ref': '', 'actual_count': 5750564, 'total_size': 25306193577, 'total_zsize': 8978094714, 'redirect_count': 0, 'other_count': 0} errors 0