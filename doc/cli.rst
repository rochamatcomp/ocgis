:tocdepth: 4

======================
Command Line Interface
======================

The OpenClimateGIS command line interface provides access to Python capabilities using command line syntax. Supported subcommands are accessed using:

.. code-block:: sh

   ocli <subcommand> <arguments>

Current subcommands:

=============== ========================== ===============================================================================================================================================================
Subcommand      Long Name                  Description
=============== ========================== ===============================================================================================================================================================
``chunked_rwg`` :ref:`chunked_rwg_section` Chunked regrid weight generation using spatial decompositions and ESMF. Allows weight generation for very high resolution grids in memory-limited environments.
=============== ========================== ===============================================================================================================================================================

.. _chunked_rwg_section:

++++++++++++++++++++++++++++++++
Chunked Regrid Weight Generation
++++++++++++++++++++++++++++++++

Chunked regrid weight generation uses a spatial decomposition to calculate regridding weights for high resolution grids. The destination grid is chunked using index-based slicing. The source grid is then spatially subset by the spatial extent of the destination chunk plus a spatial buffer to ensure the destination chunk is fully mapped by the source chunk. Weights are calculated using ESMF for each chunked source-destination combination. A global weight file merge is performed by default on the weight chunks to creating a global weights file.

In addition to chunked weight generation, the interface also offers spatial subsetting of the source grid using the `global` spatial extent of the destination grid. This is useful in situations where the destination grid spatial extent is very small compared to the spatial extent of the source grid.

-----
Usage
-----

.. tdk: LAST-CLN: update help output; don't wrap final text to save space

.. code-block:: sh

   $ ocli chunked_rwg --help

   Usage: ocli.py chunked_rwg [OPTIONS]

     Run regridding using a spatial decomposition.

   Options:
     -s, --source PATH               Path to the source grid NetCDF file.
                                     [required]
     -d, --destination PATH          Path to the destination grid NetCDF file.
                                     [required]
     -n, --nchunks_dst TEXT          Single integer or sequence defining the
                                     chunking decomposition for the destination
                                     grid. For unstructured grids, provide a
                                     single value (i.e. 100). For logically
                                     rectangular grids, two values are needed to
                                     describe the x and y decomposition (i.e.
                                     10,20).
     --merge / --no_merge            (default=True) If --merge, merge weight file
                                     chunks into a global weight file.
     -w, --weight PATH               Path to the output global weight file.
                                     Required if --merge.
     --esmf_src_type TEXT            (default=GRIDSPEC) ESMF source grid type.
                                     Supports GRIDSPEC, UGRID, and SCRIP.
     --esmf_dst_type TEXT            (default=GRIDSPEC) ESMF destination grid
                                     type. Supports GRIDSPEC, UGRID, and SCRIP.
     --genweights / --no_genweights  (default=True) Generate weights using ESMF
                                     for each source and destination subset.
     --esmf_regrid_method TEXT       (default=CONSERVE) The ESMF regrid method.
                                     Only applicable with --genweights. Supports
                                     CONSERVE and BILINEAR.
     --spatial_subset / --no_spatial_subset
                                     (default=False) Optionally subset the
                                     destination grid by the bounding box spatial
                                     extent of the source grid. This will not
                                     work in parallel if --genweights.
     --src_resolution FLOAT          Optionally overload the spatial resolution
                                     of the source grid. If provided, assumes an
                                     isomorphic structure.
     --dst_resolution FLOAT          Optionally overload the spatial resolution
                                     of the destination grid. If provided,
                                     assumes an isomorphic structure.
     --buffer_distance FLOAT         Optional spatial buffer distance (in units
                                     of the destination grid) to use when
                                     subsetting the source grid by the spatial
                                     extent of a destination grid or chunk. This
                                     is computed internally if not provided.
     --wd PATH                       Optional working directory for output
                                     intermediate files.
     --persist / --no_persist        (default=False) If --persist, do not remove
                                     the working directory --wd following
                                     execution.
     --help                          Show this message and exit.

-----------
Limitations
-----------

* Reducing memory overhead leverages IO heavily. Best performance is attained when ``netCDF4-python`` is built with parallel support to allow asynchronous IO with OpenClimateGIS.
* Supports weight generation only without weight application (sparse matrix multiplication).
* Works for spherical latitude/longitude grids only.

--------
Examples
--------

___________________________
Logically Rectangular Grids
___________________________

This example creates two global, spherical, latitude/longitude grids with differing spatial resolutions. First, we write the grids to NetCDF files. It then calls the command line chunked regrid weight generation in parallel. The destination grid is decomposed into 25 chunks - five splits along the y-axis and five splits along the x-axis.

.. literalinclude:: sphinx_examples/chunked_rwg_rect.py

_____________________________________
Weight Generation with Spatial Subset
_____________________________________

This example creates a global, spherical, latitude/longitude grid. It also creates a grid with a single cell. The spatial extent of the single cell grid is much smaller than the global grid. Both grids are written to file.

.. literalinclude:: sphinx_examples/chunked_rwg_ss.py
