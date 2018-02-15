import os

from shapely.geometry import box

from ocgis.spatial.base import iter_spatial_decomposition, create_split_polygons
from ocgis.test.base import TestBase, create_gridxy_global
from ocgis.variable.crs import Spherical


class Test(TestBase):

    def test_create_split_polygons(self):
        bbox = box(180, 30, 270, 40)
        splits = (2, 3)
        polys = create_split_polygons(bbox, splits)
        for poly in polys:
            print(poly.bounds)

    def test_iter_spatial_decomposition(self):
        self.remove_dir = False  # tdk
        print(self.current_dir_output)  # tdk
        grid = create_gridxy_global(wrapped=False, crs=Spherical())
        splits = (3, 3)
        ctr = 0
        for yld in iter_spatial_decomposition(grid, splits, optimzed_bbox_subset=True):
            print(ctr)
            yld.get_abstraction_geometry().write_vector(os.path.join(self.current_dir_output, 'shp{}.shp'.format(ctr)))
            ctr += 1
