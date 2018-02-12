import os
import subprocess
import sys
import tempfile

import ocgis
from ocgis.test.base import create_gridxy_global

DATADIR = tempfile.mkdtemp(prefix='ocgis_chunked_rwg_')
SRC_CFGRID = os.path.join(DATADIR, 'src.nc')
DST_CFGRID = os.path.join(DATADIR, 'dst.nc')
WEIGHT = os.path.join(DATADIR, 'esmf_weights.nc')
OCLI_EXE = os.path.expanduser('~/l/ocgis/src/ocli.py')

# Write the source and destination grids. The destination grid has a slightly larger resolution. -----------------------
srcgrid = create_gridxy_global(crs=ocgis.crs.Spherical())
srcgrid.parent.write(SRC_CFGRID)

# Write the destination grid. Slice the grid first to create a single cell.
dstgrid = create_gridxy_global(resolution=5, crs=ocgis.crs.Spherical())
dstgrid = dstgrid[18, 36]
dstgrid.parent.write(DST_CFGRID)

# Construct the chunked regrid weight generation command and execute in a subprocess.
# tdk: LAST-CLN: revert to ocli instead of ocli.py
cmd = [sys.executable, OCLI_EXE, 'chunked_rwg', '-s', SRC_CFGRID, '-d', DST_CFGRID, '-w', WEIGHT, '--spatial_subset']
print(' '.join(cmd))
subprocess.check_call(cmd)

# Inspect the weight file output.
ocgis.RequestDataset(WEIGHT).inspect()
