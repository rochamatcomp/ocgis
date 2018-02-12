import os
import sys

import numpy as np
from mock import mock, PropertyMock
from shapely.geometry import box

from ocgis import RequestDataset, Field, vm, env
from ocgis.base import get_variable_names
from ocgis.constants import MPIWriteMode, GridChunkerConstants, VariableName, WrappedState
from ocgis.driver.nc_ugrid import DriverNetcdfUGRID
from ocgis.spatial.grid import GridUnstruct, Grid
from ocgis.spatial.grid_chunker import GridChunker, does_contain
from ocgis.test.base import attr, AbstractTestInterface, create_gridxy_global
from ocgis.test.test_ocgis.test_driver.test_nc_scrip import FixtureDriverNetcdfSCRIP
from ocgis.variable.base import Variable
from ocgis.variable.crs import Spherical
from ocgis.variable.dimension import Dimension
from ocgis.variable.temporal import TemporalVariable
from ocgis.vmachine.mpi import MPI_COMM, MPI_RANK


class TestGridChunker(AbstractTestInterface, FixtureDriverNetcdfSCRIP):

    @property
    def fixture_paths(self):
        return {'wd': self.current_dir_output}

    @staticmethod
    def add_data_variable_to_grid(grid):
        ydim, xdim = grid.dimensions
        tdim = Dimension(name='time', size=None)
        value = np.random.rand(31, ydim.size, xdim.size)
        data = Variable(name='data', dimensions=[tdim, ydim, xdim], value=value)
        tvar = TemporalVariable(name='time', value=list(range(31)), dimensions=tdim, attrs={'axis': 'T'})
        grid.parent.add_variable(data)
        grid.parent.add_variable(tvar)

    def assertWeightFilesEquivalent(self, global_weights_filename, merged_weights_filename):
        nwf = RequestDataset(merged_weights_filename).get()
        gwf = RequestDataset(global_weights_filename).get()
        nwf_row = nwf['row'].get_value()
        gwf_row = gwf['row'].get_value()
        self.assertAsSetEqual(nwf_row, gwf_row)
        nwf_col = nwf['col'].get_value()
        gwf_col = gwf['col'].get_value()
        self.assertAsSetEqual(nwf_col, gwf_col)
        nwf_S = nwf['S'].get_value()
        gwf_S = gwf['S'].get_value()
        self.assertEqual(nwf_S.sum(), gwf_S.sum())
        unique_src = np.unique(nwf_row)
        diffs = []
        for us in unique_src.flat:
            nwf_S_idx = np.where(nwf_row == us)[0]
            nwf_col_sub = nwf_col[nwf_S_idx]
            nwf_S_sub = nwf_S[nwf_S_idx].sum()

            gwf_S_idx = np.where(gwf_row == us)[0]
            gwf_col_sub = gwf_col[gwf_S_idx]
            gwf_S_sub = gwf_S[gwf_S_idx].sum()

            self.assertAsSetEqual(nwf_col_sub, gwf_col_sub)

            diffs.append(nwf_S_sub - gwf_S_sub)
        diffs = np.abs(diffs)
        self.assertLess(diffs.max(), 1e-14)

    def fixture_grid_chunker(self, **kwargs):
        src_grid = self.get_gridxy_global(wrapped=False, with_bounds=True)
        dst_grid = self.get_gridxy_global(wrapped=False, with_bounds=True, resolution=0.5)

        self.add_data_variable_to_grid(src_grid)
        self.add_data_variable_to_grid(dst_grid)

        defaults = {'source': src_grid, 'destination': dst_grid, 'paths': self.fixture_paths, 'nchunks_dst': (2, 3)}
        defaults.update(kwargs)

        gs = GridChunker(**defaults)
        return gs

    def fixture_regular_ugrid_file(self, ufile, resolution, crs=None):
        """
        Create a UGRID convention file from a structured, rectilinear grid. This will create element centers in addition
        to element node connectivity.

        :param str ufile: Path to the output UGRID NetCDF file.
        :param float resolution: The resolution fo the structured grid to convert to UGRID.
        :param crs: The coordinate system for the UGRID data.
        :type crs: :class:`~ocgis.CRS`
        """

        src_grid = self.get_gridxy_global(resolution=resolution, crs=crs)
        polygc = src_grid.get_abstraction_geometry()
        polygc.reshape([Dimension(name='element_count', size=polygc.size)])
        polygc = polygc.convert_to(max_element_coords=4)

        pointgc = src_grid.get_point()
        pointgc.reshape([Dimension(name='element_count', size=pointgc.size)])
        pointgc = pointgc.convert_to(xname='face_center_x', yname='face_center_y')

        polygc.parent.add_variable(pointgc.parent.first(), force=True)
        polygc.parent.dimension_map.update(pointgc.dimension_map)
        pointgc.parent = polygc.parent

        gu = GridUnstruct(geoms=[polygc, pointgc])
        gu.parent.write(ufile)

    def run_system_splitting_unstructured(self, genweights):
        # tdk: LAST-FIX: fails with mpi4py import when it is not installed. should pass in serial.
        env.CLOBBER_UNITS_ON_BOUNDS = False

        ufile = self.get_temporary_file_path('ugrid.nc')
        resolution = 10.
        self.fixture_regular_ugrid_file(ufile, resolution)
        src_rd = RequestDataset(ufile, driver=DriverNetcdfUGRID, grid_abstraction='point')
        # src_rd.inspect()
        src_grid = src_rd.get().grid
        self.assertEqual(src_grid.abstraction, 'point')
        dst_grid = self.get_gridxy_global(resolution=20., crs=Spherical())

        gs = GridChunker(src_grid, dst_grid, (3, 3), check_contains=False, src_grid_resolution=10.,
                         paths=self.fixture_paths, genweights=genweights)

        gs.write_subsets()

        actual = gs.create_full_path_from_template('src_template', index=1)
        actual = RequestDataset(actual).get()
        self.assertIn(GridChunkerConstants.IndexFile.NAME_SRCIDX_GUID, actual)

    @attr('mpi', 'slow')
    def test(self):
        gs = self.fixture_grid_chunker()

        desired_dst_grid_sum = gs.dst_grid.parent['data'].get_value().sum()
        desired_dst_grid_sum = MPI_COMM.gather(desired_dst_grid_sum)
        if vm.rank == 0:
            desired_sum = np.sum(desired_dst_grid_sum)

        desired = [{'y': slice(0, 180, None), 'x': slice(0, 240, None)},
                   {'y': slice(0, 180, None), 'x': slice(240, 480, None)},
                   {'y': slice(0, 180, None), 'x': slice(480, 720, None)},
                   {'y': slice(180, 360, None), 'x': slice(0, 240, None)},
                   {'y': slice(180, 360, None), 'x': slice(240, 480, None)},
                   {'y': slice(180, 360, None), 'x': slice(480, 720, None)}]
        actual = list(gs.iter_dst_grid_slices())
        self.assertEqual(actual, desired)

        gs.write_subsets()

        if vm.rank == 0:
            rank_sums = []

        for ctr in range(1, gs.nchunks_dst[0] * gs.nchunks_dst[1] + 1):
            src_path = gs.create_full_path_from_template('src_template', index=ctr)
            dst_path = gs.create_full_path_from_template('dst_template', index=ctr)

            src_field = RequestDataset(src_path).get()
            dst_field = RequestDataset(dst_path).get()

            src_envelope_global = box(*src_field.grid.extent_global)
            dst_envelope_global = box(*dst_field.grid.extent_global)

            self.assertTrue(does_contain(src_envelope_global, dst_envelope_global))

            actual = get_variable_names(src_field.data_variables)
            self.assertIn('data', actual)

            actual = get_variable_names(dst_field.data_variables)
            self.assertIn('data', actual)
            actual_data_sum = dst_field['data'].get_value().sum()
            actual_data_sum = MPI_COMM.gather(actual_data_sum)
            if MPI_RANK == 0:
                actual_data_sum = np.sum(actual_data_sum)
                rank_sums.append(actual_data_sum)

        if vm.rank == 0:
            self.assertAlmostEqual(desired_sum, np.sum(rank_sums))
            index_path = gs.create_full_path_from_template('index_file')
            self.assertTrue(os.path.exists(index_path))

        vm.barrier()

        index_path = gs.create_full_path_from_template('index_file')
        index_field = RequestDataset(index_path).get()
        self.assertTrue(len(list(index_field.keys())) > 2)

    def test_init(self):
        # Test optimizations are chosen appropriately.
        grid = mock.create_autospec(Grid)
        grid.ndim = 2
        grid.resolution_max = 10
        self.assertIsInstance(grid, Grid)
        gridu = mock.create_autospec(GridUnstruct)
        gridu.resolution_max = None
        self.assertIsInstance(gridu, GridUnstruct)
        for g in [grid, gridu]:
            g._gc_initialize_ = mock.Mock()

        gs = GridChunker(gridu, grid, (3, 4), paths=self.fixture_paths)
        self.assertFalse(gs.optimized_bbox_subset)

        gs = GridChunker(gridu, grid, (3, 4), src_grid_resolution=1.0, dst_grid_resolution=2.0,
                         paths=self.fixture_paths)
        self.assertTrue(gs.optimized_bbox_subset)

        # Test ESMF keyword arguments.
        mock_ESMF = mock.Mock()
        with mock.patch.dict(sys.modules, {'ESMF': mock_ESMF}):
            gs = self.fixture_grid_chunker(genweights=True)
            self.assertGreaterEqual(len(gs.esmf_kwargs), 2)
            self.assertTrue(gs.genweights)

    def test_system_regrid_target_types(self):
        """Test grids are retrieved from the supported input regrid target types."""

        mGrid = mock.create_autospec(Grid, spec_set=True, instance=True)
        type(mGrid).ndim = PropertyMock(return_value=2)

        def _create_mField_():
            mField = mock.create_autospec(Field, spec_set=True, instance=True)
            p_grid = PropertyMock(return_value=mGrid)
            type(mField).grid = p_grid
            return mField, p_grid

        def _create_mRequestDataset_():
            ret = mock.create_autospec(RequestDataset, spec_set=True, instance=True)
            ret.create_field = mock.Mock(return_value=_create_mField_()[0])
            return ret

        # Test with request datasets.
        source = _create_mRequestDataset_()
        destination = _create_mRequestDataset_()
        gs = GridChunker(source, destination, (1, 1))
        for t in [source, destination]:
            t.create_field.assert_called_once()
        for t in [gs.src_grid, gs.dst_grid]:
            self.assertEqual(t, mGrid)

        # Test with fields.
        source, psource = _create_mField_()
        destination, pdestination = _create_mField_()
        gs = GridChunker(source, destination, (1, 1))
        for t in [psource, pdestination]:
            t.assert_called_once_with()
        for t in [gs.src_grid, gs.dst_grid]:
            self.assertEqual(t, mGrid)

    def test_system_scrip_destination_splitting(self):
        """Test splitting a SCRIP destination grid."""

        src_grid = create_gridxy_global()
        dst_grid = self.fixture_driver_scrip_netcdf_field().grid
        gs = GridChunker(src_grid, dst_grid, (3,), paths={'wd': self.current_dir_output})
        gs.write_subsets()
        self.assertEqual(len(os.listdir(self.current_dir_output)), 7)

    def test_system_splitting_unstructured_no_weights(self):
        """Only split the unstructured grid source."""
        self.run_system_splitting_unstructured(False)

    @attr('esmf')
    def test_system_splitting_unstructured_with_weights(self):
        """Only split the unstructured grid source."""
        self.run_system_splitting_unstructured(True)
        # subprocess.check_call(['tree', self.current_dir_output])

    @attr('esmf')
    def test_create_merged_weight_file(self):
        import ESMF

        path_src = self.get_temporary_file_path('src.nc')
        path_dst = self.get_temporary_file_path('dst.nc')

        src_grid = create_gridxy_global(resolution=30.0, wrapped=False, crs=Spherical())
        dst_grid = create_gridxy_global(resolution=35.0, wrapped=False, crs=Spherical())

        src_grid.write(path_src)
        dst_grid.write(path_dst)

        # Split source and destination grids ---------------------------------------------------------------------------

        gs = GridChunker(src_grid, dst_grid, (2, 2), check_contains=False, allow_masked=True, paths=self.fixture_paths,
                         genweights=True)
        gs.write_subsets()

        # Merge weight files -------------------------------------------------------------------------------------------

        merged_weight_filename = self.get_temporary_file_path('merged_weights.nc')
        gs.create_merged_weight_file(merged_weight_filename)

        # Generate a global weight file using ESMF ---------------------------------------------------------------------

        global_weights_filename = self.get_temporary_file_path('global_weights.nc')

        srcgrid = ESMF.Grid(filename=path_src, filetype=ESMF.FileFormat.GRIDSPEC, add_corner_stagger=True)
        dstgrid = ESMF.Grid(filename=path_dst, filetype=ESMF.FileFormat.GRIDSPEC, add_corner_stagger=True)
        srcfield = ESMF.Field(grid=srcgrid)
        dstfield = ESMF.Field(grid=dstgrid)
        _ = ESMF.Regrid(srcfield=srcfield, dstfield=dstfield, filename=global_weights_filename,
                        regrid_method=ESMF.RegridMethod.CONSERVE)

        # Test merged and global weight files are equivalent -----------------------------------------------------------

        self.assertWeightFilesEquivalent(global_weights_filename, merged_weight_filename)

    @attr('esmf')
    def test_create_merged_weight_file_unstructured(self):
        import ESMF

        # Create an isomorphic source UGRID file.
        ufile = self.get_temporary_file_path('ugrid.nc')
        resolution = 10.
        self.fixture_regular_ugrid_file(ufile, resolution, crs=Spherical())
        src_grid = RequestDataset(ufile, driver=DriverNetcdfUGRID, grid_abstraction='point').get().grid
        self.assertEqual(src_grid.abstraction, 'point')

        # Create a logically rectangular destination grid file.
        dst_grid = self.get_gridxy_global(resolution=20., crs=Spherical())
        dst_path = self.get_temporary_file_path('dst.nc')
        dst_grid.parent.write(dst_path)

        # Create the grid chunks.
        gs = GridChunker(src_grid, dst_grid, (3, 3), check_contains=False, src_grid_resolution=10.,
                         paths=self.fixture_paths, genweights=True)
        gs.write_subsets()

        # Merge weight files.
        mwf = self.get_temporary_file_path('merged_weight_file.nc')
        gs.create_merged_weight_file(mwf)

        # Generate a global weight file using ESMF.
        global_weights_filename = self.get_temporary_file_path('global_weights.nc')
        srcgrid = ESMF.Mesh(filename=ufile, filetype=ESMF.FileFormat.UGRID, meshname=VariableName.UGRID_HOST_VARIABLE)
        dstgrid = ESMF.Grid(filename=dst_path, filetype=ESMF.FileFormat.GRIDSPEC, add_corner_stagger=True)
        srcfield = ESMF.Field(grid=srcgrid, meshloc=ESMF.MeshLoc.ELEMENT)
        dstfield = ESMF.Field(grid=dstgrid)
        _ = ESMF.Regrid(srcfield=srcfield, dstfield=dstfield, filename=global_weights_filename,
                        regrid_method=ESMF.RegridMethod.CONSERVE)

        # Test merged and global weight files are equivalent.
        self.assertWeightFilesEquivalent(global_weights_filename, mwf)

    @attr('slow')
    def test_insert_weighted(self):
        gs = self.fixture_grid_chunker()

        dst_master_path = self.get_temporary_file_path('out.nc')
        gs.dst_grid.parent.write(dst_master_path)

        dst_master = RequestDataset(dst_master_path).get()
        desired_sums = {}
        for data_variable in dst_master.data_variables:
            dv_sum = data_variable.get_value().sum()
            desired_sums[data_variable.name] = dv_sum
            self.assertNotEqual(dv_sum, 0)
            data_variable.get_value()[:] = 0
        dst_master.write(dst_master_path, write_mode=MPIWriteMode.FILL)
        dst_master = RequestDataset(dst_master_path).get()
        for data_variable in dst_master.data_variables:
            self.assertEqual(data_variable.get_value().sum(), 0)

        gs.write_subsets()

        index_path = gs.create_full_path_from_template('index_file')
        gs.insert_weighted(index_path, self.current_dir_output, dst_master_path)

        actual_sums = {}
        dst_master_inserted = RequestDataset(dst_master_path).get()
        for data_variable in dst_master_inserted.data_variables:
            dv_value = data_variable.get_value()
            dv_sum = dv_value.sum()
            actual_sums[data_variable.name] = dv_sum
        for k, v in list(actual_sums.items()):
            self.assertAlmostEqual(v, desired_sums[k])

    def test_nchunks_dst(self):
        gc = self.fixture_grid_chunker(nchunks_dst=None)
        self.assertIsNotNone(gc.nchunks_dst)
        self.assertEqual(gc.nchunks_dst, (10, 10))

    def test_write_subsets(self):
        # Test a destination iterator.
        grid = mock.create_autospec(GridUnstruct)
        grid._gc_initialize_ = mock.Mock()
        grid.resolution_max = 10.0
        grid.ndim = 1
        grid.wrapped_state = WrappedState.UNWRAPPED
        grid.crs = Spherical()
        grid.get_intersects = mock.Mock(return_value=(grid, slice(0, 0)))
        grid.is_empty = False

        parent = mock.create_autospec(Field, spec_set=True)
        parent.add_variable = mock.Mock()
        grid.parent = parent

        grid.shape_global = (5,)

        def iter_dst(grid_chunker, yield_slice=False):
            for _ in range(3):
                yld = mock.create_autospec(GridUnstruct, instance=True)
                yld.wrapped_state = WrappedState.UNWRAPPED
                yld.is_empty = False
                yld.extent_global = (0, 0, 0, 0)

                parent = mock.create_autospec(Field, spec_set=True)
                parent.add_variable = mock.Mock()
                yld.parent = parent
                yld.shape_global = (25,)
                yld.extent_global = (1, 2, 3, 4)
                yld.parent.attrs = {}

                if yield_slice:
                    yld = yld, None
                yield yld

        gs = GridChunker(grid, grid, (100,), iter_dst=iter_dst, dst_grid_resolution=5.0, check_contains=False)
        gs.write_subsets()
