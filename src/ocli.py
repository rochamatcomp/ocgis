#!/usr/bin/env python
import shutil
import tempfile

import click


# tdk: DOC: add src_type and dst_type to RWG ESMF documentation
# tdk: ENH: added defaults for nchunks_dst
# tdk: LAST: harmonize GridSplitter param names with final interface names
import os

import ocgis
from ocgis import RequestDataset, GridSplitter
from ocgis.constants import DriverKey


@click.group()
def ocli():
    pass


@ocli.command(help='Chunk two grids using a spatial decomposition.')
@click.option('-s', '--source', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the source grid NetCDF file.')
@click.option('-d', '--destination', required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the destination grid NetCDF file.')
@click.option('-w', '--weight', required=True, type=click.Path(exists=False, dir_okay=False),
              help='Path to the output global weight file or prefix for the weight file.')
@click.option('-n', '--nchunks_dst',
              help='Single integer or sequence defining the chunking decomposition for the destination grid.')
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
@click.option('--persist/--no_persist', default=False,
              help='If present, do not remove the working directory --wd following execution.')
@click.option('--merge/--no_merge', default=False,
              help='If present, do not merge weight file chunks into a global weight file.')
def chunker(source, destination, weight, nchunks_dst, esmf_src_type, esmf_dst_type, src_resolution, dst_resolution, buffer_distance, wd, persist, merge):
    # Format the chunking decomposition from its string representation.
    if ',' in nchunks_dst:
        nchunks_dst = nchunks_dst.split(',')
    else:
        nchunks_dst = [nchunks_dst]
    nchunks_dst = tuple([int(ii) for ii in nchunks_dst])

    # Create the source and destination request datasets.
    rd_src = create_request_dataset(source, esmf_src_type)
    rd_dst = create_request_dataset(destination, esmf_dst_type)

    # Make a temporary working directory is one is not provided by the client. Only do this if we are writing subsets
    # and it is not a merge only operation.
    if not merge:
        if wd is None:
            if ocgis.vm.rank == 0:
                wd = tempfile.mkdtemp(prefix='ocgis_chunker_', dir=os.getcwd())
            wd = ocgis.vm.bcast(wd)
        else:
            # The working directory must not exist to proceed.
            if os.path.exists(wd):
                raise ValueError("Working directory 'wd' must not exist.")
            else:
                # Make the working directory nesting as needed.
                if ocgis.vm.rank == 0:
                    os.makedirs(wd)
                ocgis.vm.barrier()

    # Update the paths to use for the grid chunker.
    paths = {'wd': wd}
    # If we are not merging the chunked weight files, the weight string value is the string template to use for the
    # output weight files.
    if not merge:
        paths['wgt_template'] = weight

    gs = GridSplitter(rd_src, rd_dst, nchunks_dst, src_grid_resolution=src_resolution, paths=paths,
                      dst_grid_resolution=dst_resolution, buffer_value=buffer_distance)

    # Create the global weight file.
    if merge:
        gs.create_merged_weight_file(weight)
    else:
        # Write the subsets. Only do this if this is not a merge operation.
        gs.write_subsets()

    # Remove the working directory unless the persist flag is provided.
    if not persist:
        if ocgis.vm.rank == 0:
            shutil.rmtree(wd)
        ocgis.vm.barrier()

    return 0


def create_request_dataset(path, esmf_type):
    # tdk: need scrip driver
    edmap = {'GRIDSPEC': DriverKey.NETCDF_CF,
             'UGRID': DriverKey.NETCDF_UGRID,
             'SCRIP': 'tkk'}
    odriver = edmap[esmf_type]
    return RequestDataset(uri=path, driver=odriver)


if __name__ == '__main__':
    ocli()
