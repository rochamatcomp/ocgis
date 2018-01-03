from ocgis import DimensionMap, env
from ocgis.constants import DriverKey, DMK, Topology
from ocgis.driver.nc import DriverNetcdf
from ocgis.exc import GridDeficientError
from ocgis.variable.crs import Spherical


class DriverScripNetcdf(DriverNetcdf):
    # tdk: DOC
    # tdk: RENAME: DriverNetcdfSCRIP
    key = DriverKey.NETCDF_SCRIP
    _default_crs = env.DEFAULT_COORDSYS

    def create_dimension_map(self, group_metadata, **kwargs):
        #tdk: need to account for bounds
        ret = DimensionMap()
        ret.set_driver(self)
        topo = ret.get_topology(Topology.POINT, create=True)
        topo.set_variable(DMK.X, 'grid_center_lon', dimension='grid_size')
        topo.set_variable(DMK.Y, 'grid_center_lat', dimension='grid_size')
        return ret

    @staticmethod
    def get_grid(field):
        from ocgis import GridUnstruct
        try:
            ret = GridUnstruct(parent=field)
        except GridDeficientError:
            ret = None
        return ret