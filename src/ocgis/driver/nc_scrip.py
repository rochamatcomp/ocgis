import numpy as np

from ocgis import DimensionMap, env, constants, vm
from ocgis.constants import DriverKey, DMK, Topology, MPIOps
from ocgis.driver.base import AbstractUnstructuredDriver
from ocgis.driver.nc import DriverNetcdf
from ocgis.util.helpers import create_unique_global_array
from ocgis.vmachine.mpi import hgather


class DriverScripNetcdf(AbstractUnstructuredDriver, DriverNetcdf):
    # tdk: DOC
    # tdk: RENAME: DriverNetcdfSCRIP

    _esmf_filetype = 'SCRIP'
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
        #tdk: RESUME: need to account for bounds
        ret = DimensionMap()
        ret.set_driver(self)

        topo = ret.get_topology(Topology.POINT, create=True)
        topo.set_variable(DMK.X, 'grid_center_lon', dimension='grid_size')
        topo.set_variable(DMK.Y, 'grid_center_lat', dimension='grid_size')

        if 'grid_corner_lon' in group_metadata['variables']:
            topo = ret.get_topology(Topology.POLYGON, create=True)
            topo.set_variable(DMK.X, 'grid_corner_lon', dimension='grid_size')
            topo.set_variable(DMK.Y, 'grid_corner_lat', dimension='grid_size')

        # The isomorphic property covers all possible mesh topologies.
        ret.set_property(DMK.IS_ISOMORPHIC, True)

        return ret

    def get_distributed_dimension_name(self, dimension_map, dimensions_metadata):
        return 'grid_size'

    @classmethod
    def _get_field_write_target_(cls, field):
        # tdk: CLEAN
        # ux = np.unique(sub['grid_center_lon'].get_value()).shape[0]
        # uy = np.unique(sub['grid_center_lat'].get_value()).shape[0]

        # Unstructured SCRIP has a value of 1 for the grid dimensions by default. Just leave it alone.
        if field.dimensions['grid_rank'].size > 1:
            # Update the grid size based on unique x/y values. In SCRIP, the coordinate values are duplicated in the
            # coordinate vector.
            ux = field.grid.x.shape[0]
            uy = field.grid.y.shape[0]
            field['grid_dims'].get_value()[:] = ux, uy
        return field

    @staticmethod
    def _gs_iter_dst_grid_slices_(grid_splitter):
        # tdk: CLEAN
        # tdk: HACK: this method uses some global gathers which is not ideal
        # Destination splitting works off center coordinates only.
        pgc = grid_splitter.dst_grid.abstractions_available['point']

        # Use the unique center values to break the grid into pieces. This ensures that nearby grid cell are close
        # spatially. If we just break the grid into pieces w/out using unique values, the points may be scattered which
        # does not optimize the spatial coverage of the source grid.
        center_lat = pgc.y.get_value()
        # center_lat = pgc.parent['grid_center_lat'].get_value()

        # ucenter_lat = np.unique(center_lat)
        ucenter_lat = create_unique_global_array(center_lat)

        # ocgis_lh(msg=['ucenter_lat=', ucenter_lat], logger='tdk', level=10)

        ucenter_lat = vm.gather(ucenter_lat)
        if vm.rank == 0:
            ucenter_lat = hgather(ucenter_lat)
            ucenter_lat.sort()
            ucenter_splits = np.array_split(ucenter_lat, grid_splitter.nchunks_dst[0])
        else:
            ucenter_splits = [None] * grid_splitter.nchunks_dst[0]

        # ocgis_lh(msg=['ucenter_splits=', ucenter_splits], logger='tdk', level=10)

        # for ctr, ucenter_split in enumerate(ucenter_splits, start=1):
        for ucenter_split in ucenter_splits:

            ucenter_split = vm.bcast(ucenter_split)

            select = np.zeros_like(center_lat, dtype=bool)
            for v in ucenter_split.flat:
                select = np.logical_or(select, center_lat == v)
            # sub = pgc.parent[{pgc.node_dim.name: select}]
            # split_path = os.path.join(WD, 'split_dst_{}.nc').format(ctr)

            # ux = np.unique(sub['grid_center_lon'].get_value()).shape[0]
            # uy = np.unique(sub['grid_center_lat'].get_value()).shape[0]
            # sub['grid_dims'].get_value()[:] = ux, uy

            # with ocgis.vm.scoped('grid write', [0]):
            #     if not ocgis.vm.is_null:
            #         sub.write(split_path, driver='netcdf')
            # ocgis.vm.barrier()

            # yld = create_scrip_grid(split_path)

            # if yield_slice:
            #     yld = yld, ucenter_split
            # yield yld
            yield select

    @staticmethod
    def _gs_nchunks_dst_(grid_splitter):
        pgc = grid_splitter.dst_grid.abstractions_available['point']
        y = pgc.y.get_value()
        uy = create_unique_global_array(y)
        total = vm.reduce(uy.size, MPIOps.SUM)
        total = vm.bcast(total)
        if total < 100:
            ret = total
        else:
            ret = 100
        return ret
