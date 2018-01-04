import numpy as np
from mock import mock

from ocgis import RequestDataset, DimensionMap, Grid, GridUnstruct
from ocgis.constants import DriverKey, DMK, Topology
from ocgis.driver.nc_scrip import DriverScripNetcdf
from ocgis.test.base import TestBase
from ocgis.variable.crs import Spherical


class TestDriverScripNetcdf(TestBase):

    def test_init(self):
        rd = mock.create_autospec(RequestDataset)
        d = DriverScripNetcdf(rd)
        self.assertIsInstance(d, DriverScripNetcdf)

    def test_create_field(self):
        # tdk: test with bounds and corners handled
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
