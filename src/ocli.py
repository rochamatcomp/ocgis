#!/usr/bin/env python
# tdk: DOC: add src_type and dst_type to RWG ESMF documentation
# tdk: ENH: add defaults for nchunks_dst
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


@ocli.command(help='Execute regridding using a spatial decomposition.')
@click.option('-s', '--source', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the source grid NetCDF file.')
@click.option('-d', '--destination', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the destination grid NetCDF file.')
@click.option('-n', '--nchunks_dst',
              help='Single integer or sequence defining the chunking decomposition for the destination grid. For '
                   'unstructured grids, provide a single value (i.e. 100). For logically rectangular grids, two values '
                   'are needed to describe the x and y decomposition (i.e. 10,20).')
@click.option('--merge/--no_merge', default=True,
              help='(default=True) If --merge, merge weight file chunks into a global weight file.')
@click.option('-w', '--weight', required=False, type=click.Path(exists=False, dir_okay=False),
              help='Path to the output global weight file. Required if --merge.')
@click.option('--esmf_src_type', type=str, nargs=1, default='GRIDSPEC',
              help='(default=GRIDSPEC) ESMF source grid type.')
@click.option('--esmf_dst_type', type=str, nargs=1, default='GRIDSPEC',
              help='(default=GRIDSPEC) ESMF destination grid type.')
@click.option('--genweights/--no_genweights', default=True,
              help='(default=True) Generate weights using ESMF for each source and destination subset.')
@click.option('--esmf_regrid_method', type=str, nargs=1, default='CONSERVE',
              help='(default=CONSERVE) The ESMF regrid method. Only applicable with --genweights.')
@click.option('--src_resolution', type=float, nargs=1,
              help='Optionally overload the spatial resolution of the source grid. If provided, assumes an isomorphic '
                   'structure.')
@click.option('--dst_resolution', type=float, nargs=1,
              help='Optionally overload the spatial resolution of the destination grid. If provided, assumes an '
                   'isomorphic structure.')
@click.option('--buffer_distance', type=float, nargs=1,
              help='Optional spatial buffer distance (in units of the destination grid) to use when subsetting '
                   'the source grid by the spatial extent of a destination grid or chunk. This is computed internally '
                   'if not provided.')
@click.option('--wd', type=click.Path(exists=False), default=None,
              help='Optional working directory for output intermediate files.')
@click.option('--persist/--no_persist', default=False,
              help='(default=False) If --persist, do not remove the working directory --wd following execution.')
@click.option('--spatial_subset/--no_spatial_subset', default=False,
              help='(default=False) Optionally Subset the destination grid by the bounding box spatial extent of the '
                   'source grid. This will not work in parallel if --genweights.')
def chunked_regrid(source, destination, weight, nchunks_dst, merge, esmf_src_type, esmf_dst_type, genweights,
                   esmf_regrid_method, src_resolution, dst_resolution, buffer_distance, wd, persist, spatial_subset):
    # tdk: REMOVE
    # ocgis.env.VERBOSE = True
    # ocgis.env.DEBUG = True
    # ocgis.env.configure_logging(with_header=False)
    # tdk: /REMOVE

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
            wd = tempfile.mkdtemp(prefix='ocgis_chunked_regrid_')
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

    # Arguments to ESMF regridding.
    esmf_kwargs = {'regrid_method': esmf_regrid_method}

    # Create the chunked regridding object. This is used for both chunked regridding and a regrid with a spatial subset.
    gs = GridSplitter(rd_src, rd_dst, nchunks_dst=nchunks_dst, src_grid_resolution=src_resolution, paths=paths,
                      dst_grid_resolution=dst_resolution, buffer_value=buffer_distance, redistribute=True,
                      genweights=genweights, esmf_kwargs=esmf_kwargs)

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
