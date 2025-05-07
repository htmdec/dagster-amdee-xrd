# Dagster AMDEE XRD

This repository defines a Dagster pipeline for monitoring and processing **X-ray Diffraction (XRD) sample data** hosted in [Girder](https://girder.readthedocs.io/). It supports automatic discovery of new samples, dynamic partitioning of raw XRD samples, and scalable analysis using Docker-based execution. The pipeline also includes custom Girder-based IO managers, automated data ingestion, processing, and asset materialization. At a high level, the pipeline is structured as follows:
1. Finds new XRD scans in Girder
2. Preprocesses and analyzes them (e.g., cleans images, plots them, extracts data)
3. Uploads the processed results (images + CSVs) back to Girder
4. Keeps track of what was processed and when (avoiding duplicates)

All of this is orchestrated using Dagster, a Python-based data workflow tool that handles jobs, schedules, data lineage, and metadata.

## System Overview

| File                 | Description                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------ |
| `assets.py`          | Dagster asset definitions for raw data ingestion and observation scheduling.               |
| `definitions.py`     | Dagster `Definitions` object that declares assets, jobs, schedules, and resources.         |
| `resources.py`       | Custom Dagster resources to interact with the Girder API.                                  |
| `utils.py`           | Utility functions for image processing, detector configuration, and XRD-specific analysis. |
| `image_utilities.py` | Preprocessing helpers such as background subtraction and hot pixel removal.                |
| `__init__.py`        | Entrypoint that loads the Dagster `defs` object for deployment.                            |

## Module Features
`assets.py`: 

Defines a Dagster data pipeline for managing and analyzing X-ray diffraction (XRD) sample data stored in Girder, a data management system. The pipeline is structured around assets, partitions, jobs, and schedules, and integrates with Docker for execution.
How it works:
- Monitors Girder for new XRD sample data
- Registers new samples as dynamic partitions
- Triggers automated processing for unprocessed samples
- Runs analysis inside Docker containers using provided credentials and folders
- Schedules checks every minute to keep the pipeline updated

`definitions.py`:

Ties together the Dagster pipeline components into a complete deployment-ready setup using the Definitions object. It handles asset registration, job and schedule inclusion, and resource configuration.
How it works:
- Loads all XRD processing assets into Dagster
- Configures jobs and schedules for automatic polling and processing
- Sets up resources to interact with Girder and the local filesystem
- Defines everything in a central Definitions object so the pipeline is ready to deploy and execute

`resources.py`:

Defines all necessary resource classes and a custom I/O manager for interacting with Girder, a data management platform. It allows Dagster to load, save, and track data assets directly through Girder APIs.
How it works:
- Authenticates and manages sessions with Girder via GirderConnection.
- Provides high-level helper methods for retrieving and uploading files.
- Implements a custom Dagster I/O Manager (GirderIOManager) to:
  - Automatically name, version, and upload analysis outputs.
  - Retrieve input files cleanly and consistently.
  - Generate rich metadata for observability in the Dagster UI.
- Enables configurable resource registration through ConfigurableGirderIOManager, making it ready for scalable and secure deployment.

`utils.py`:

Handles local preprocessing, image transformation, azimuthal integration, and file upload for X-ray diffraction (XRD) scans retrieved from Girder. It defines the XRDAnalysis class used in your Dagster asset and performs end-to-end analysis of HDF5 detector output.
How it works:
- Defines the entire XRD scan analysis logic, from raw detector data to publishable plots.
- Cleans, preprocesses, and visualizes Eiger detector images.
- Performs azimuthal integration to extract meaningful 1D intensity plots from 2D diffraction rings.
- Efficiently manages temporary file I/O during analysis.
- Uploads results to Girder, linking them with metadata and provenance (wasDerivedFrom, wasGeneratedBy).
- Prevents redundant analysis using version checks, making it reproducible and idempotent.

`image_utilities.py`:

Provides reusable image filtering, background subtraction, and radial integration functions for processing raw 2D X-ray diffraction images. It supports cleaning up noisy detector images and converting them into 1D intensity profiles (𝑰 vs. 2θ) for scientific analysis.
How it works:
- Provides a compact and effective image denoising pipeline for XRD patterns.
- Performs azimuthal integration to convert raw 2D images into 1D intensity profiles.
- Corrects for pixel area and geometric distortion, ensuring physically meaningful output.
- Excludes corrupted low-angle bins and standardizes output for downstream CSV or plot generation.
- Used internally by the XRDAnalysis class and easily pluggable into broader scientific workflows.

### How They Connect
1. Scheduler (in `repository.py`) runs every minute.
2. It triggers `raw_xrd_data` (in `assets.py`) to check Girder for new samples.
3. If there are new or unprocessed samples:
    - Dagster automatically triggers `xrd_samples`.
    - This calls `XRDAnalysis.analyze()` (in `xrd_analysis.py`).
    - The analysis downloads files from Girder, runs image cleaning + integration, and creates plots and CSVs.
4. The results are uploaded back to Girder using `GirderConnection.upload_file_to_folder(...)`.
5. Each asset logs metadata in Dagster — so you can track exactly what was run, when, on what sample, and with what version.

## Technologies Used
1. Dagster: data orchestration & scheduling
2. Girder: data storage & metadata management
3. h5py: reading .h5 detector files
4. matplotlib: plotting diffraction images & results
5. NumPy: array computations
6. scipy: filtering for denoising
