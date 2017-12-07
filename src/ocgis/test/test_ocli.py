from unittest import SkipTest

import mock

import ocgis
from ocgis import RequestDataset, GridSplitter
from ocgis.constants import DriverKey
from ocgis.test.base import TestBase, attr
from click.testing import CliRunner

from ocgis.util.addict import Dict
from ocli import ocli, chunker


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
        poss.weight = [self.get_temporary_file_path('weights.nc'), self.get_temporary_file_path('weight_chunk_')]
        poss.nchunks_dst = ['1,1', '1']
        poss.esmf_src_type = ['__exclude__', 'GRIDSPEC']
        poss.esmf_dst_type = ['__exclude__', 'GRIDSPEC']
        poss.src_resolution = ['__exclude__', '1.0']
        poss.dst_resolution = ['__exclude__', '2.0']
        poss.buffer_distance = ['__exclude__', '3.0']
        poss.wd = ['__exclude__', self.get_temporary_file_path('wd')]
        poss.persist = ['__exclude__', '__include__']
        poss.no_merge = ['__exclude__', '__include__']

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

        m_mkdtemp.return_value = 'mkdtemp return value'

        poss = self.fixture_flags_good()
        for ctr, k in enumerate(self.iter_product_keywords(poss, as_namedtuple=False), start=1):
            new_poss = {}
            for k2, v2 in k.items():
                if v2 != '__exclude__':
                    new_poss[k2] = v2
            cli_args = ['chunker']
            for k2, v2 in new_poss.items():
                cli_args.append('--{}'.format(k2))
                if v2 != '__include__':
                    cli_args.append(v2)

            # print cli_args
            runner = CliRunner()
            result = runner.invoke(ocli, args=cli_args)
            self.assertEqual(result.exit_code, 0)

            mGridSplitter.assert_called_once()
            instance = mGridSplitter.return_value
            instance.write_subsets.assert_called_once()
            call_args = mGridSplitter.call_args
            if k['wd'] == '__exclude__':
                actual = call_args[1]['paths']['wd']
                self.assertEqual(actual, m_mkdtemp.return_value)
            if 'no_merge' not in new_poss:
                instance.create_merged_weight_file.assert_called_once_with(new_poss['weight'])
            else:
                self.assertEqual(call_args[1]['paths']['wgt_template'], k['weight'])
                instance.create_merged_weight_file.assert_not_called()
            if k['nchunks_dst'] == '1,1':
                self.assertEqual(call_args[0][2], (1, 1))
            else:
                self.assertEqual(call_args[0][2], (1,))

            self.assertEqual(mRequestDataset.call_count, 2)

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
            if 'persist' not in new_poss:
                if ocgis.vm.rank == 0:
                    m_rmtree.assert_called_once()
                else:
                    m_rmtree.assert_not_called()
            else:
                m_rmtree.assert_not_called()

            mocks = [mRequestDataset, mGridSplitter, m_mkdtemp, m_rmtree, m_makedirs]
            for m in mocks:
                m.reset_mock()
