import numpy as np
from mock import mock

from ocgis import RequestDataset, DimensionMap, Grid, GridUnstruct, PointGC, Field, Variable, Dimension
from ocgis.constants import DriverKey, DMK, Topology
from ocgis.driver.nc_scrip import DriverScripNetcdf
from ocgis.test.base import TestBase, create_gridxy_global
from ocgis.variable.crs import Spherical

import itertools


class FixtureDriverScripNetcdf(object):

    def fixture_driver_scrip_netcdf_field(self):
        xvalue = np.arange(10., 35., step=5)
        yvalue = np.arange(45., 85., step=10)
        grid_size = xvalue.shape[0] * yvalue.shape[0]

        dim_grid_size = Dimension(name='grid_size', size=grid_size)
        x = Variable(name='grid_center_lon', dimensions=dim_grid_size)
        y = Variable(name='grid_center_lat', dimensions=dim_grid_size)

        for idx, (xv, yv) in enumerate(itertools.product(xvalue, yvalue)):
            x.get_value()[idx] = xv
            y.get_value()[idx] = yv

        gc = PointGC(x=x, y=y, crs=Spherical(), driver=DriverScripNetcdf)
        grid = GridUnstruct(geoms=[gc])
        ret = Field(grid=grid, driver=DriverScripNetcdf)

        grid_dims = Variable(name='grid_dims', value=[yvalue.shape[0], xvalue.shape[0]], dimensions='grid_rank')
        ret.add_variable(grid_dims)

        return ret


class TestDriverScripNetcdf(TestBase, FixtureDriverScripNetcdf):

    def test_init(self):
        rd = mock.create_autospec(RequestDataset)
        d = DriverScripNetcdf(rd)
        self.assertIsInstance(d, DriverScripNetcdf)

        field = self.fixture_driver_scrip_netcdf_field()
        self.assertIsInstance(field, Field)

        #tdk: REMOVE
        slc = np.array([1, 3])
        sub = field.grid.get_distributed_slice(slc)
        pass
        #tdk: /REMOVE

    def test_system_spatial_resolution(self):
        """Test spatial resolution is computed appropriately."""
        # tdk: REMOVE: this test is for development and can be taken away at the end
        path = '/mnt/e24fbd51-d3a4-44e5-82a5-0e20b3487199/data/bekozi-work/i49-ugrid-cesm/0.9x1.25_c110307.nc'
        rd = RequestDataset(path, driver=DriverKey.NETCDF_SCRIP)
        field = rd.create_field()
        self.assertEqual(field.driver.key, DriverKey.NETCDF_SCRIP)
        self.assertEqual(field.grid.driver.key, DriverKey.NETCDF_SCRIP)

        self.barrier_print(field.shapes)

        self.assertEqual(field.grid.resolution_x, 1.25)
        self.assertAlmostEqual(field.grid.resolution_y, 0.94240837696335089)

    def test_array_resolution(self):
        self.assertEqual(DriverScripNetcdf.array_resolution(np.array([5]), None), 0.0)
        self.assertEqual(DriverScripNetcdf.array_resolution(np.array([-5, -10, 10, 5], dtype=float), None), 5.0)

    def test_array_resolution_called(self):
        """Test the driver's array resolution method is called appropriately."""

        m_DriverScripNetcdf = mock.create_autospec(DriverScripNetcdf)
        with mock.patch('ocgis.driver.registry.get_driver_class', return_value=m_DriverScripNetcdf):
            x = Variable(name='x', value=[1, 2, 3], dimensions='dimx')
            y = Variable(name='y', value=[4, 5, 6], dimensions='dimy')
            pgc = PointGC(x=x, y=y)
            _ = pgc.resolution_x
            _ = pgc.resolution_y
        self.assertEqual(m_DriverScripNetcdf.array_resolution.call_count, 2)

    def test_create_field(self):
        # tdk: test with bounds and corners handled
        # tdk: RESUME: test that the data is distributed appropriately when loading in a scrip file in parallel
        meta = {'dimensions': {u'grid_corners': {'isunlimited': False,
                                  'name': u'grid_corners',
                                  'size': 4},
                u'grid_rank': {'isunlimited': False,
                               'name': u'grid_rank',
                               'size': 2},
                u'grid_size': {'isunlimited': False,
                               'name': u'grid_size',
                               'size': 55296}},
 'file_format': 'NETCDF3_CLASSIC',
 'global_attributes': {u'input_file': u'/fs/cgd/csm/inputdata/lnd/clm2/griddata/griddata_0.9x1.25_c070928.nc',
                       u'title': u'0.9x1.25_c110307.nc'},
 'groups': {},
 'variables': {u'grid_center_lat': {'attrs': {u'units': u'degrees'},
                                    'dimensions': (u'grid_size',),
                                    'dtype': np.dtype('float64'),
                                    'dtype_packed': None,
                                    'fill_value': 'auto',
                                    'fill_value_packed': None,
                                    'name': u'grid_center_lat'},
               u'grid_center_lon': {'attrs': {u'units': u'degrees'},
                                    'dimensions': (u'grid_size',),
                                    'dtype': np.dtype('float64'),
                                    'dtype_packed': None,
                                    'fill_value': 'auto',
                                    'fill_value_packed': None,
                                    'name': u'grid_center_lon'},
               u'grid_corner_lat': {'attrs': {u'units': u'degrees'},
                                    'dimensions': (u'grid_size',
                                                   u'grid_corners'),
                                    'dtype': np.dtype('float64'),
                                    'dtype_packed': None,
                                    'fill_value': 'auto',
                                    'fill_value_packed': None,
                                    'name': u'grid_corner_lat'},
               u'grid_corner_lon': {'attrs': {u'units': u'degrees'},
                                    'dimensions': (u'grid_size',
                                                   u'grid_corners'),
                                    'dtype': np.dtype('float64'),
                                    'dtype_packed': None,
                                    'fill_value': 'auto',
                                    'fill_value_packed': None,
                                    'name': u'grid_corner_lon'},
               u'grid_dims': {'attrs': {},
                              'dimensions': (u'grid_rank',),
                              'dtype': np.dtype('int32'),
                              'dtype_packed': None,
                              'fill_value': 'auto',
                              'fill_value_packed': None,
                              'name': u'grid_dims'},
               u'grid_imask': {'attrs': {u'units': u'unitless'},
                               'dimensions': (u'grid_size',),
                               'dtype': np.dtype('int32'),
                               'dtype_packed': None,
                               'fill_value': 'auto',
                               'fill_value_packed': None,
                               'name': u'grid_imask'}}}

        rd = RequestDataset(metadata=meta, driver=DriverScripNetcdf)
        d = DriverScripNetcdf(rd)

        dmap = d.create_dimension_map(meta)
        self.assertIsInstance(dmap, DimensionMap)

        actual = dmap.get_property(DMK.IS_ISOMORPHIC)
        self.assertTrue(actual)

        field = d.create_field()

        self.assertEqual(field.crs, Spherical())
        self.assertEqual(field.driver.key, DriverKey.NETCDF_SCRIP)
        self.assertIsInstance(field.grid, GridUnstruct)
        desired = meta['dimensions']['grid_size']['size']
        actual = field.grid.element_dim.size
        self.assertEqual(desired, actual)
        self.assertTrue(field.grid.is_isomorphic)
