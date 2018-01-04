from ocgis import DimensionMap, env, constants
from ocgis.constants import DriverKey, DMK, Topology
from ocgis.driver.base import AbstractUnstructuredDriver
from ocgis.driver.nc import DriverNetcdf
from ocgis.exc import GridDeficientError
from ocgis.variable.crs import Spherical
import numpy as np


class DriverScripNetcdf(AbstractUnstructuredDriver, DriverNetcdf):
    # tdk: DOC
    # tdk: RENAME: DriverNetcdfSCRIP
    key = DriverKey.NETCDF_SCRIP
    _default_crs = env.DEFAULT_COORDSYS

    @staticmethod
    def array_resolution(value, axis):
        # tdk: doc
        if value.size == 1:
            return 0.0
        else:
            resolution_limit = constants.RESOLUTION_LIMIT
            value = np.sort(np.unique(np.abs(value)))
            value = value[0:resolution_limit]
            value = np.diff(value)
            ret = np.mean(value)
            return ret

    def create_dimension_map(self, group_metadata, **kwargs):
        #tdk: need to account for bounds
        ret = DimensionMap()
        ret.set_driver(self)

        topo = ret.get_topology(Topology.POINT, create=True)
        topo.set_variable(DMK.X, 'grid_center_lon', dimension='grid_size')
        topo.set_variable(DMK.Y, 'grid_center_lat', dimension='grid_size')

        # The isomorphic property covers all possible mesh topologies.
        ret.set_property(DMK.IS_ISOMORPHIC, True)

        return ret
