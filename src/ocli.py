#!/usr/bin/env python
# tdk: DOC: add src_type and dst_type to RWG ESMF documentation
# tdk: ENH: added defaults for nchunks_dst
# tdk: LAST: harmonize GridSplitter param names with final interface names
import os
import shutil
import tempfile
from logging import DEBUG

import click
from shapely.geometry import box

import ocgis
from ocgis import RequestDataset, GridSplitter, GeometryVariable
from ocgis.base import grid_abstraction_scope
from ocgis.constants import DriverKey, Topology, GridSplitterConstants
from ocgis.spatial.spatial_subset import SpatialSubsetOperation
from ocgis.util.logging_ocgis import ocgis_lh


@click.group()
def ocli():
    pass


@ocli.command(help='CESM grid manipulations to assist in regridding.')
@click.option('-s', '--source', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the source grid NetCDF file.')
@click.option('-d', '--destination', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the destination grid NetCDF file.')
@click.option('-n', '--nchunks_dst',
              help='Single integer or sequence defining the chunking decomposition for the destination grid.')
@click.option('-w', '--weight', required=False, type=click.Path(exists=False, dir_okay=False),
              help='Path to the output global weight file.')
@click.option('--esmf_src_type', type=str, nargs=1, default='GRIDSPEC',
              help='ESMF source grid type.')
@click.option('--esmf_dst_type', type=str, nargs=1, default='GRIDSPEC',
              help='ESMF destination grid type.')
@click.option('--src_resolution', type=float, nargs=1,
              help='Spatial resolution for the source grid. If provided, assumes a rectilinear structure.')
@click.option('--dst_resolution', type=float, nargs=1,
              help='Spatial resolution for the destination grid. If provided, assumes a rectilinear structure.')
@click.option('--buffer_distance', type=float, nargs=1,
              help='Specifies the spatial buffer distance to use when subsetting the source grid by the spatial extent '
                   'of a destination grid or chunk.')
@click.option('--wd', type=click.Path(exists=False), default=None,
              help='Base working directory for output intermediate files.')
@click.option('--persist/--no_persist', default=True,
              help='If --persist, do not remove the working directory --wd following execution.')
@click.option('--merge/--no_merge', default=True,
              help='If --no_merge, do not merge weight file chunks into a global weight file.')
@click.option('--spatial_subset/--no_spatial_subset', default=False,
              help='Subset the destination grid by the bounding box spatial extent of the source grid.')
@click.option('--genweights/--no_genweights', default=True,
              help='Generate weights using ESMPy for each source and destination subset.')
def cesm_manip(source, destination, weight, nchunks_dst, esmf_src_type, esmf_dst_type, src_resolution, dst_resolution,
               buffer_distance, wd, persist, merge, spatial_subset, genweights):
    # tdk: LAST: RENAME: to ESMPy_RegridWeightGen?

    # tdk: REMOVE
    ocgis.env.VERBOSE = True
    ocgis.env.DEBUG = True
    # tdk: /REMOVE
    ocgis.env.configure_logging(with_header=False)

    if nchunks_dst is not None:
        # Format the chunking decomposition from its string representation.
        if ',' in nchunks_dst:
            nchunks_dst = nchunks_dst.split(',')
        else:
            nchunks_dst = [nchunks_dst]
        nchunks_dst = tuple([int(ii) for ii in nchunks_dst])
    if merge:
        if not spatial_subset and weight is None:
            raise ValueError('"weight" must be a valid path if --merge')
    if spatial_subset and genweights and weight is None:
        raise ValueError('"weight" must be a valid path if --genweights')

    # Make a temporary working directory is one is not provided by the client. Only do this if we are writing subsets
    # and it is not a merge only operation.
    if wd is None:
        if ocgis.vm.rank == 0:
            wd = tempfile.mkdtemp(prefix='ocgis_cesm_manip_', dir=os.getcwd())
        wd = ocgis.vm.bcast(wd)
    else:
        if ocgis.vm.rank == 0:
            # The working directory must not exist to proceed.
            if os.path.exists(wd):
                raise ValueError("Working directory 'wd' must not exist.")
            else:
                # Make the working directory nesting as needed.
                os.makedirs(wd)
        ocgis.vm.barrier()

    if merge and not spatial_subset or (spatial_subset and genweights):
        if _is_subdir_(wd, weight):
            raise ValueError(
                'Merge weight file path must not in the working directory. It may get unintentionally deleted with the --no_persist flag.')

    # Create the source and destination request datasets.
    rd_src = _create_request_dataset_(source, esmf_src_type)
    rd_dst = _create_request_dataset_(destination, esmf_dst_type)

    # tdk: need option to spatially subset and apply weights
    # Execute a spatial subset if requested.
    paths = None
    if spatial_subset:
        # tdk: should this be a parameter?
        spatial_subset_path = os.path.join(wd, 'spatial_subset.nc')
        _write_spatial_subset_(rd_src, rd_dst, spatial_subset_path)
        # tdk: /hack
    # Only split grids if a spatial subset is not requested.
    else:
        # Update the paths to use for the grid.
        paths = {'wd': wd}

    # Create the chunked regridding object. This is used for both chunked regridding and a regrid with a spatial subset.
    gs = GridSplitter(rd_src, rd_dst, nsplits_dst=nchunks_dst, src_grid_resolution=src_resolution, paths=paths,
                      dst_grid_resolution=dst_resolution, buffer_value=buffer_distance, redistribute=True,
                      genweights=genweights)

    # Write subsets and generate weights if requested in the grid splitter.
    # tdk: need a weight only option; currently subsets are always written and so is the merged weight file
    if not spatial_subset and nchunks_dst is not None:
        gs.write_subsets()
    else:
        if spatial_subset:
            source = spatial_subset_path
        if genweights:
            gs.write_esmf_weights(source, destination, weight)

    # Create the global weight file. This does not apply to spatial subsets because there will always be one weight
    # file.
    if merge and not spatial_subset:
        # Weight file merge only works in serial.
        with ocgis.vm.scoped('weight file merge', [0]):
            if not ocgis.vm.is_null:
                gs.create_merged_weight_file(weight)
        ocgis.vm.barrier()

    # elif not spatial_subset:
    #     # Write the subsets. Only do this if this is not a merge operation.
    #     gs.write_subsets()
    # else:
    #     assert spatial_subset
    #     assert not merge

    # Remove the working directory unless the persist flag is provided.
    if not persist:
        if ocgis.vm.rank == 0:
            shutil.rmtree(wd)
        ocgis.vm.barrier()

    return 0


def _create_request_dataset_(path, esmf_type):
    edmap = {'GRIDSPEC': DriverKey.NETCDF_CF,
             'UGRID': DriverKey.NETCDF_UGRID,
             'SCRIP': DriverKey.NETCDF_SCRIP}
    odriver = edmap[esmf_type]
    # tdk: HACK: the abstraction target should be determine by is_isomorphic=True and point is available. this requires
    # tdk: HACK:  an is_isomorphic argument to be passed through the request dataset
    return RequestDataset(uri=path, driver=odriver, grid_abstraction='point')


def _is_subdir_(path, potential_subpath):
    # https://stackoverflow.com/questions/3812849/how-to-check-whether-a-directory-is-a-sub-directory-of-another-directory#18115684
    path = os.path.realpath(path)
    potential_subpath = os.path.realpath(potential_subpath)
    relative = os.path.relpath(path, potential_subpath)
    return not relative.startswith(os.pardir + os.sep)


def _write_spatial_subset_(rd_src, rd_dst, spatial_subset_path):
    # tdk: HACK: this is sensitive and should be replaced with more robust code. there is also an opportunity to simplify subsetting by incorporating the spatial subset operation object into subsetting itself.
    src_field = rd_src.create_field()
    dst_field = rd_dst.create_field()
    sso = SpatialSubsetOperation(src_field)

    with grid_abstraction_scope(dst_field.grid, Topology.POLYGON):
        dst_field_extent = dst_field.grid.extent_global

    ocgis_lh(logger='ocli', msg=['src_field.grid.resolution_max', src_field.grid.resolution_max], level=DEBUG)
    print
    subset_geom = GeometryVariable.from_shapely(box(*dst_field_extent), crs=dst_field.crs, is_bbox=True)
    sub_src = sso.get_spatial_subset('intersects', subset_geom,
                                     buffer_value=GridSplitterConstants.BUFFER_RESOLUTION_MODIFIER * src_field.grid.resolution_max,
                                     optimized_bbox_subset=True)

    # Try to reduce the coordinate indexing for unstructured grids.
    try:
        reduced = sub_src.grid.reduce_global()
    except AttributeError:
        pass
    else:
        sub_src = reduced.parent

    sub_src.write(spatial_subset_path)


if __name__ == '__main__':
    ocli()
