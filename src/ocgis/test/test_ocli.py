import numpy as np

from ocgis import env
import subprocess
from unittest import SkipTest

import mock
import os

import ocgis
from ocgis import RequestDataset, GridSplitter, Variable, Grid
from ocgis.constants import DriverKey
from ocgis.test.base import TestBase, attr, create_gridxy_global, create_exact_field
from click.testing import CliRunner

from ocgis.util.addict import Dict
from ocgis.variable.crs import Spherical
from ocli import ocli, cesm_manip


#tdk: modify nose testing implementation to not load this file? how to deal with click?

@attr('cli')
class Test(TestBase):

    def fixture_flags_good(self):
        poss = Dict()

        source = self.get_temporary_file_path('source.nc')
        with open(source, 'w') as f:
            f.write('foo')

        destination = self.get_temporary_file_path('destination.nc')
        with open(destination, 'w') as f:
            f.write('foo')

        poss.source = [source]
        poss.destination = [destination]
        poss.nchunks_dst = ['1,1', '1']
        poss.esmf_src_type = ['__exclude__', 'GRIDSPEC']
        poss.esmf_dst_type = ['__exclude__', 'GRIDSPEC']
        poss.src_resolution = ['__exclude__', '1.0']
        poss.dst_resolution = ['__exclude__', '2.0']
        poss.buffer_distance = ['__exclude__', '3.0']
        poss.wd = ['__exclude__', self.get_temporary_file_path('wd')]
        poss.no_persist = ['__exclude__', '__include__']
        poss.merge = ['__exclude__', '__include__']

        return poss

    def test_init(self):
        runner = CliRunner()
        result = runner.invoke(ocli)
        self.assertEqual(result.exit_code, 0)

    @mock.patch('os.makedirs')
    @mock.patch('shutil.rmtree')
    @mock.patch('tempfile.mkdtemp')
    @mock.patch('ocli.GridSplitter')
    @mock.patch('ocli.RequestDataset')
    @attr('mpi')
    def test_system_mock_combinations(self, mRequestDataset, mGridSplitter, m_mkdtemp, m_rmtree, m_makedirs):
        if ocgis.vm.size not in [1, 2]:
            raise SkipTest('ocgis.vm.size not in [1, 2]')

        poss_weight = {'filename': self.get_temporary_file_path('weights.nc'),
                       'prefix': self.get_temporary_file_path('weight_chunk_')}

        m_mkdtemp.return_value = 'mkdtemp return value'

        poss = self.fixture_flags_good()
        for ctr, k in enumerate(self.iter_product_keywords(poss, as_namedtuple=False), start=1):
            new_poss = {}
            for k2, v2 in k.items():
                if v2 != '__exclude__':
                    new_poss[k2] = v2
            cli_args = ['cesm_manip']
            for k2, v2 in new_poss.items():
                cli_args.append('--{}'.format(k2))
                if v2 != '__include__':
                    cli_args.append(v2)

            if 'merge' in new_poss:
                weight = poss_weight['filename']
            else:
                weight = poss_weight['prefix']
            new_poss['weight'] = weight
            cli_args.extend(['--weight', weight])

            # print cli_args
            runner = CliRunner()
            result = runner.invoke(ocli, args=cli_args)
            self.assertEqual(result.exit_code, 0)

            mGridSplitter.assert_called_once()
            instance = mGridSplitter.return_value
            call_args = mGridSplitter.call_args

            if k['wd'] == '__exclude__' and 'merge' not in new_poss:
                actual = call_args[1]['paths']['wd']
                self.assertEqual(actual, m_mkdtemp.return_value)

            if 'merge' in new_poss:
                instance.create_merged_weight_file.assert_called_once_with(new_poss['weight'])
            else:
                self.assertEqual(call_args[1]['paths']['wgt_template'], new_poss['weight'])
                instance.create_merged_weight_file.assert_not_called()
                instance.write_subsets.assert_called_once()

            if k['nchunks_dst'] == '1,1':
                self.assertEqual(call_args[0][2], (1, 1))
            else:
                self.assertEqual(call_args[0][2], (1,))

            self.assertEqual(mRequestDataset.call_count, 2)

            if 'merge' not in new_poss:
                if 'wd' not in new_poss:
                    if ocgis.vm.rank == 0:
                        m_mkdtemp.assert_called_once()
                    else:
                        m_mkdtemp.assert_not_called()
                else:
                    if ocgis.vm.rank == 0:
                        m_makedirs.assert_called_once()
                    else:
                        m_makedirs.assert_not_called()
            else:
                m_mkdtemp.assert_not_called()
                m_makedirs.assert_not_called()

            if 'no_persist' in new_poss:
                if ocgis.vm.rank == 0:
                    m_rmtree.assert_called_once()
                else:
                    m_rmtree.assert_not_called()
            else:
                m_rmtree.assert_not_called()

            mocks = [mRequestDataset, mGridSplitter, m_mkdtemp, m_rmtree, m_makedirs]
            for m in mocks:
                m.reset_mock()

    def assertWeightFilesEquivalent(self, global_weights_filename, merged_weights_filename):
        # tdk: this is duplicated in TestGridSplitter. find way to remove duplicate code.
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

    @attr('mpi')
    def test_cesm_manip(self):
        # tdk: needs to work in parallel
        env.CLOBBER_UNITS_ON_BOUNDS = False

        if ocgis.vm.size not in [1, 4]:
            raise SkipTest('ocgis.vm.size not in [1, 4]')

        src_grid = create_gridxy_global(resolution=15)
        # tdk: consider using slightly different resolutions
        dst_grid = create_gridxy_global(resolution=12)

        src_field = create_exact_field(src_grid, 'foo', crs=Spherical())
        dst_field = create_exact_field(dst_grid, 'foo', crs=Spherical())

        if ocgis.vm.rank == 0:
            source = self.get_temporary_file_path('source.nc')
        else:
            source = None
        source = ocgis.vm.bcast(source)
        src_field.write(source)
        if ocgis.vm.rank == 0:
            destination = self.get_temporary_file_path('destination.nc')
        else:
            destination = None
        destination = ocgis.vm.bcast(destination)
        dst_field.write(destination)

        # Generate the source and destination chunks.
        runner = CliRunner()
        wd = os.path.join(self.current_dir_output, 'chunks')
        cli_args = ['cesm_manip', '--source', source, '--destination', destination, '--nchunks_dst', '2,3', '--wd', wd]
        result = runner.invoke(ocli, args=cli_args, catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(len(os.listdir(wd)) > 3)

        # Generate weights for each source and destination combination.
        src_template = 'split_src_{}.nc'
        dst_template = 'split_dst_{}.nc'
        wgt_template = 'esmf_weights_{}.nc'
        for ii in range(1, 7):
            src_path = os.path.join(wd, src_template.format(ii))
            dst_path = os.path.join(wd, dst_template.format(ii))
            wgt_path = os.path.join(wd, wgt_template.format(ii))
            # tdk: why is the regional flag needed? i thought this was not needed.
            cmd = ['ESMF_RegridWeightGen', '-s', src_path, '--src_type', 'GRIDSPEC', '-d', dst_path, '--dst_type',
                   'GRIDSPEC', '-w', wgt_path, '--method', 'conserve', '--no-log', '-r', '--weight-only']
            subprocess.check_call(cmd)

        # Create a global weights file from the individual weight files.
        merged_weights = os.path.join(wd, 'merged_weights.nc')
        cli_args = ['cesm_manip', '--source', source, '--destination', destination, '--wd', wd, '--nchunks_dst', '2,3',
                    '--merge', '--weight', merged_weights]
        result = runner.invoke(ocli, args=cli_args, catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

        # Create a standard ESMF weights file from the original grid files.
        esmf_weights_path = self.get_temporary_file_path('esmf_desired_weights.nc')
        cmd = ['ESMF_RegridWeightGen', '-s', source, '--src_type', 'GRIDSPEC', '-d', destination, '--dst_type',
               'GRIDSPEC', '-w', esmf_weights_path, '--method', 'conserve', '--no-log']
        subprocess.check_call(cmd)

        self.assertWeightFilesEquivalent(esmf_weights_path, merged_weights)

    def test_cesm_manip_spatial_subset(self):
        dst_grid = create_gridxy_global()
        dst_field = create_exact_field(dst_grid, 'foo')

        xvar = Variable(name='x', value=[-90.], dimensions='xdim')
        yvar = Variable(name='y', value=[40.],  dimensions='ydim')
        src_grid = Grid(x=xvar, y=yvar)

        if ocgis.vm.rank == 0:
            source = self.get_temporary_file_path('source.nc')
        else:
            source = None
        source = ocgis.vm.bcast(source)
        src_grid.write(source)

        if ocgis.vm.rank == 0:
            destination = self.get_temporary_file_path('destination.nc')
        else:
            destination = None
        destination = ocgis.vm.bcast(destination)
        dst_field.write(destination)

        runner = CliRunner()
        wd = os.path.join(self.current_dir_output, 'chunks')
        cli_args = ['cesm_manip', '--source', source, '--destination', destination, '--wd', wd, '--spatial_subset']
        result = runner.invoke(ocli, args=cli_args, catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)

        dst_path = os.path.join(wd, 'spatial_subset.nc')

        actual = RequestDataset(uri=dst_path).create_field()
        actual_ymean = actual.grid.get_value_stacked()[0].mean()
        actual_xmean = actual.grid.get_value_stacked()[1].mean()
        self.assertEqual(actual_ymean, 40.)
        self.assertEqual(actual_xmean, -90.)
        self.assertEqual(actual.grid.shape, (4, 4))